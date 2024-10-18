use dashmap::DashMap;
use itertools::Itertools;
use memmap2::Mmap;
use pco::standalone::simple_decompress;
use postcard::from_bytes;

use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
use std::{fs, io, usize};
use tokio::task::JoinSet;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::wolfey_metrics::{AggChartRequest, Aggregation, NonAggChartRequest};

const PARTITIONS_FILE_HEADER_FILENAME: &str = "WolfeyPartitionsConfig";
const MIN_PTS_TO_COMPRESS: usize = 10_000;
const MIN_STREAMS_PER_THREAD: usize = 1000;

#[derive(Debug, Copy, Clone)]
pub struct Datapoint {
    pub unix_s: i64,
    pub value: f32,
}

struct DownsampledDatapoint {
    time_index: usize,
    value: f32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StorageConfig {
    pub compression_level: usize,
    pub retention_period_s: usize,
    pub cold_storage_after_s: usize,
    pub data_frequency_s: usize,
    pub stream_cache_ttl_s: usize,
    pub data_storage_dir: PathBuf,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
struct DiskStreamFileHeader {
    stream_id: u64,
    unix_s_byte_start: usize,
    unix_s_byte_stop: usize,
    values_byte_stop: usize,
    compressed: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct TimePartitionFileHeader {
    start_unix_s: i64,
    file_path: PathBuf,
    disk_streams: Vec<DiskStreamFileHeader>,
}

impl TimePartitionFileHeader {
    fn new(config: &StorageConfig) -> Self {
        let now = Utc::now().timestamp();
        let file_path = config.data_storage_dir.join(now.to_string());
        Self {
            start_unix_s: now,
            file_path,
            disk_streams: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PartitionsFileHeader {
    // sorted descending start_unix_s
    time_partitions: Vec<TimePartitionFileHeader>,
}

#[derive(Default)]
struct HotStream {
    unix_seconds: Vec<i64>,
    values: Vec<f32>,
}

struct Stream {
    disk_header: DiskStreamFileHeader,
    hot_stream: RwLock<Option<HotStream>>,
    last_accessed: Mutex<Option<i64>>,
}

struct TimePartition {
    start_unix_s: i64,
    mmap: Mmap,
    streams: DashMap<u64, Stream>,
}

pub struct Storage {
    config: StorageConfig,
    partitions: RwLock<Vec<Arc<TimePartition>>>,
    partition_file_header: PartitionsFileHeader,
    num_threads: usize,
}

#[derive(Clone, Copy)]
struct ChartReqMetadata {
    start_unix_s: i64,
    stop_unix_s: i64,
    step_s: u32,
    resolution: usize,
}

#[inline]
fn resolution(start_unix_s: i64, stop_unix_s: i64, step_s: u32) -> usize {
    let duration_s = (stop_unix_s - start_unix_s) as u32;
    let resolution = duration_s / step_s;
    return resolution as usize;
}

impl ChartReqMetadata {
    fn from_agg_chart_req(value: &AggChartRequest) -> Self {
        Self {
            start_unix_s: value.start_unix_s,
            stop_unix_s: value.stop_unix_s,
            step_s: value.step_s,
            resolution: resolution(value.start_unix_s, value.stop_unix_s, value.step_s),
        }
    }
    fn from_non_agg_chart_req(value: &NonAggChartRequest) -> Self {
        Self {
            start_unix_s: value.start_unix_s,
            stop_unix_s: value.stop_unix_s,
            step_s: value.step_s,
            resolution: resolution(value.start_unix_s, value.stop_unix_s, value.step_s),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            compression_level: 8,
            retention_period_s: 31104000,  //1y
            cold_storage_after_s: 7776000, //90d
            data_frequency_s: 900,
            stream_cache_ttl_s: 900,
            data_storage_dir: PathBuf::from("/var/lib/wolfeymetrics"),
        }
    }
}

const MIN_COMPRESSION_LEVEL: usize = 4;
const MAX_COMPRESSION_LEVEL: usize = 12;
const MIN_RETENTION_PERIOD_S: usize = 900; //15m
const MIN_COLD_STORAGE_S: usize = 7776000; //90d
const MAX_RETENTION_PERIOD_S: usize = 3156000000; //100y
const MAX_DATA_FREQUENCY_S: usize = 604800; //7d

#[derive(thiserror::Error, Debug)]
pub enum StorageConfigError {
    #[error("COMPRESSION_LEVEL must be >= {MIN_COMPRESSION_LEVEL}")]
    ToLowCompressionLevel,
    #[error("COMPRESSION_LEVEL must be <= {MAX_COMPRESSION_LEVEL}")]
    ToHighCompressionLevel,
    #[error("RETENTION_PERIOD must be >= {MIN_RETENTION_PERIOD_S}")]
    ToLowRetentionPeriod,
    #[error("RETENTION_PERIOD must be <= {MAX_RETENTION_PERIOD_S}")]
    ToHighRetentionPeriod,
    #[error("RETENTION_PERIOD_S must be >= COLD_STORAGE_AFTER_S")]
    ColdStorageCannotBeGreaterThanRetention,
    #[error("COLD_STORAGE_AFTER_S must be >= {MIN_COLD_STORAGE_S} or RETENTION_PERIOD_S")]
    ColdStorageTooLow,
    #[error("DATA_FREQUENCY_S must be <= {MAX_DATA_FREQUENCY_S}")]
    DataFrequencyTooHigh,
}

impl StorageConfig {
    fn validate(self) -> Result<Self, StorageConfigError> {
        if self.compression_level < MIN_COMPRESSION_LEVEL {
            return Err(StorageConfigError::ToLowCompressionLevel);
        }

        if self.compression_level > MAX_COMPRESSION_LEVEL {
            return Err(StorageConfigError::ToHighCompressionLevel);
        }

        if self.retention_period_s < MIN_RETENTION_PERIOD_S {
            return Err(StorageConfigError::ToLowRetentionPeriod);
        }
        if self.retention_period_s > MAX_RETENTION_PERIOD_S {
            return Err(StorageConfigError::ToHighRetentionPeriod);
        }

        if self.retention_period_s < self.cold_storage_after_s {
            return Err(StorageConfigError::ColdStorageCannotBeGreaterThanRetention);
        }

        let min_cold_storage_s = std::cmp::min(MIN_COLD_STORAGE_S, self.retention_period_s);

        if self.cold_storage_after_s < min_cold_storage_s {
            return Err(StorageConfigError::ColdStorageTooLow);
        }

        if self.data_frequency_s > MAX_DATA_FREQUENCY_S {
            return Err(StorageConfigError::DataFrequencyTooHigh);
        }

        Ok(self)
    }
}

impl DiskStreamFileHeader {
    fn read_stream_from_mmap(&self, mmap: &Mmap) -> HotStream {
        let unix_s_bytes = &mmap[self.unix_s_byte_start..self.unix_s_byte_stop];
        let value_bytes = &mmap[self.unix_s_byte_stop..self.values_byte_stop];

        let Ok(unix_s_decompressed) = simple_decompress(unix_s_bytes) else {
            return HotStream::default();
        };
        let Ok(values_decompressed) = simple_decompress(value_bytes) else {
            return HotStream::default();
        };

        HotStream {
            unix_seconds: unix_s_decompressed,
            values: values_decompressed,
        }
    }
}

impl HotStream {
    fn get_chart_aggregated(
        &self,
        req: ChartReqMetadata,
        aggregated_result: &mut [Option<f32>],
        agg: Aggregation,
    ) {
        let start_iter = self
            .unix_seconds
            .binary_search(&req.start_unix_s)
            .unwrap_or_else(|x| x);
        let stop_iter = self
            .unix_seconds
            .binary_search(&req.stop_unix_s)
            .unwrap_or_else(|x| x);

        for (i, unix_s) in self.unix_seconds[start_iter..stop_iter].iter().enumerate() {
            todo!()
        }
    }
}

impl Stream {
    async fn get_chart_aggregated(
        &self,
        req: ChartReqMetadata,
        mmap: &Mmap,
        aggregated_result: &mut [Option<f32>],
        agg: Aggregation,
    ) {
        let hot_stream_option = self.hot_stream.read().await;
        if let Some(ref x) = *hot_stream_option {
            x.get_chart_aggregated(req, aggregated_result, agg);
            return;
        }
        drop(hot_stream_option);

        let mut writable_hot_stream = self.hot_stream.write().await;

        if let Some(ref x) = *writable_hot_stream {
            x.get_chart_aggregated(req, aggregated_result, agg);
            return;
        }

        let hot_stream = self.disk_header.read_stream_from_mmap(mmap);

        hot_stream.get_chart_aggregated(req, aggregated_result, agg);
        *writable_hot_stream = Some(hot_stream);
    }
}

async fn get_chart_aggregated_batched(
    req: Arc<AggChartRequest>,
    meta: ChartReqMetadata,
    agg: Aggregation,
    thread_idx: usize,
    num_threads: usize,
    time_partition: Arc<TimePartition>,
) -> Vec<Option<f32>> {
    let streams_per_thread = req.stream_ids.len() / num_threads;
    let start_idx = streams_per_thread * thread_idx;
    let stop_idx = if thread_idx == num_threads - 1 {
        req.stream_ids.len()
    } else {
        streams_per_thread * (thread_idx + 1)
    };
    // TODO double check the previous math

    let streams = req.stream_ids[start_idx..stop_idx]
        .iter()
        .filter_map(|x| time_partition.streams.get(x));

    let mut aggregated_batch: Vec<Option<f32>> = vec![None; meta.resolution];
    for stream in streams {
        stream
            .get_chart_aggregated(meta, &time_partition.mmap, &mut aggregated_batch, agg)
            .await;
    }
    return aggregated_batch;
}

async fn time_partition_get_agg_chart(
    time_partition: Arc<TimePartition>,
    req: Arc<AggChartRequest>,
    agg: Aggregation,
    num_threads: usize,
) -> Vec<Datapoint> {
    let meta = ChartReqMetadata::from_agg_chart_req(&req);

    let threads_requested = req.stream_ids.len() / MIN_STREAMS_PER_THREAD;
    let threads_capped = std::cmp::min(threads_requested, num_threads);
    let num_threads = std::cmp::max(threads_capped, 1);

    let batches = (0..num_threads).map(|x| {
        get_chart_aggregated_batched(
            req.clone(),
            meta,
            agg,
            x,
            num_threads,
            time_partition.clone(),
        )
    });

    let results_in_agg_batches = JoinSet::from_iter(batches).join_all().await;

    todo!()
}

impl TryFrom<&TimePartitionFileHeader> for TimePartition {
    type Error = io::Error;
    fn try_from(value: &TimePartitionFileHeader) -> Result<Self, Self::Error> {
        let hash_map = DashMap::new();
        for x in &value.disk_streams {
            hash_map.insert(
                x.stream_id,
                Stream {
                    disk_header: x.clone(),
                    hot_stream: RwLock::new(None),
                    last_accessed: Mutex::new(None),
                },
            );
        }
        let file = fs::File::open(&value.file_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            start_unix_s: value.start_unix_s,
            streams: hash_map,
            mmap: mmap,
        })
    }
}

impl PartitionsFileHeader {
    fn new(config: &StorageConfig) -> Self {
        Self {
            time_partitions: vec![TimePartitionFileHeader::new(config)],
        }
    }
    fn thaw(&self, config: &StorageConfig) -> Result<Vec<Arc<TimePartition>>, io::Error> {
        let now = Utc::now().timestamp();
        let cutoff = now - config.cold_storage_after_s as i64;
        self.time_partitions
            .iter()
            .filter(|x| x.start_unix_s > cutoff)
            .map(|x| x.try_into())
            .process_results(|iter| iter.map(Arc::new).collect())
    }
}

impl Storage {
    async fn get_partitions_in_range(
        &self,
        start_unix_s: i64,
        stop_unix_s: i64,
    ) -> Vec<Arc<TimePartition>> {
        let mut partition_end = Utc::now().timestamp();
        let mut partitions_in_range = Vec::new();
        let partitions = self.partitions.read().await;
        for partition in partitions.iter() {
            if start_unix_s > partition_end {
                return partitions_in_range;
            }
            partition_end = partition.start_unix_s;
            if stop_unix_s < partition.start_unix_s {
                continue;
            }
            partitions_in_range.push(partition.clone());
        }
        return partitions_in_range;
    }

    pub async fn get_agg_chart(&self, req: AggChartRequest, agg: Aggregation) -> Vec<Datapoint> {
        let time_partitions = self
            .get_partitions_in_range(req.start_unix_s, req.stop_unix_s)
            .await;
        let arc_req = Arc::new(req);
        let datapoint_jobs = time_partitions
            .into_iter()
            .map(|x| time_partition_get_agg_chart(x, arc_req.clone(), agg, self.num_threads));
        let datapoints_nested = JoinSet::from_iter(datapoint_jobs).join_all().await;
        let datapoints_flattened = datapoints_nested.into_iter().flatten().collect();
        return datapoints_flattened;
    }

    pub fn new(
        config: StorageConfig,
        num_threads: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = config.validate()?;
        let partitions_file_path = config
            .data_storage_dir
            .join(PARTITIONS_FILE_HEADER_FILENAME);

        let partition_file_header: PartitionsFileHeader = if partitions_file_path.exists() {
            let partitions_file_header_bytes = std::fs::read(partitions_file_path)?;
            from_bytes(&partitions_file_header_bytes)?
        } else {
            PartitionsFileHeader::new(&config)
        };

        let partitions = RwLock::new(partition_file_header.thaw(&config)?);

        Ok(Self {
            config,
            partitions,
            partition_file_header,
            num_threads: num_threads,
        })
    }
}
