use dashmap::DashMap;
use pco::standalone::simple_decompress;
use std::str::FromStr;
use std::sync::Arc;
use std::usize;
use std::{fmt::Debug, path::PathBuf};
use tokio::task::JoinSet;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;

use crate::wolfey_metrics::{AggChartRequest, Aggregation, NonAggChartRequest};

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
    pub min_pts_to_compress: usize,
    pub default_compression_level: usize,
    pub retention_period_s: usize,
    pub data_frequency_s: usize,
    pub stream_cache_ttl_s: usize,
    pub data_storage_dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct DiskStreamFileHeader {
    stream_id: u64,
    unix_s_byte_start: usize,
    unix_s_byte_stop: usize,
    values_byte_stop: usize,
    compressed: bool,
}

#[derive(Serialize, Deserialize)]
struct TimePartitionFileHeader {
    start_unix_s: i64,
    file_path: PathBuf,
    disk_streams: Vec<DiskStreamFileHeader>,
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

struct CompressedStream {
    unix_s_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
}

struct Stream {
    disk_header: DiskStreamFileHeader,
    compressed_stream: RwLock<Option<CompressedStream>>,
    hot_stream: RwLock<Option<HotStream>>,
}

struct TimePartition {
    start_unix_s: i64,
    file_path: PathBuf,
    streams: DashMap<u64, Stream>,
}

pub struct Storage {
    config: StorageConfig,
    partitions: Vec<Arc<TimePartition>>,
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
            min_pts_to_compress: 10_000,
            default_compression_level: 8,
            retention_period_s: 7776000,
            data_frequency_s: 900,
            stream_cache_ttl_s: 900,
            data_storage_dir: PathBuf::from("/var/lib/wolfeymetrics"),
        }
    }
}

impl DiskStreamFileHeader {
    async fn get_compressed_stream_from_disk(&self, file_path: &PathBuf) -> CompressedStream {
        todo!()
    }
}

impl CompressedStream {
    fn decompress(&self) -> HotStream {
        let Ok(unix_s_decompressed) = simple_decompress(&self.unix_s_bytes) else {
            return HotStream::default();
        };
        let Ok(values_decompressed) = simple_decompress(&self.value_bytes) else {
            return HotStream::default();
        };

        HotStream {
            unix_seconds: unix_s_decompressed,
            values: values_decompressed,
        }
    }
}

impl HotStream {
    async fn hotstream_transmit_agg_chart(
        &self,
        req: ChartReqMetadata,
        tx: &Sender<DownsampledDatapoint>,
    ) {
        todo!()
    }
}

impl Stream {
    async fn stream_transmit_agg_chart(
        &self,
        req: ChartReqMetadata,
        tx: &Sender<DownsampledDatapoint>,
        file_path: &PathBuf,
    ) {
        let hot_stream_option = self.hot_stream.read().await;
        if let Some(ref x) = *hot_stream_option {
            x.hotstream_transmit_agg_chart(req, &tx).await;
            return;
        }
        drop(hot_stream_option);

        let mut writable_hot_stream = self.hot_stream.write().await;

        if let Some(ref x) = *writable_hot_stream {
            x.hotstream_transmit_agg_chart(req, &tx).await;
            return;
        }

        let compressed_stream_option = self.compressed_stream.read().await;

        if let Some(ref x) = *compressed_stream_option {
            let hot_stream = x.decompress();
            *writable_hot_stream = Some(hot_stream);
            return;
        }

        drop(compressed_stream_option);

        let mut writable_compressed_stream = self.compressed_stream.write().await;

        if let Some(ref x) = *writable_compressed_stream {
            let hot_stream = x.decompress();
            *writable_hot_stream = Some(hot_stream);
            return;
        }

        let compressed_stream_from_disk = self
            .disk_header
            .get_compressed_stream_from_disk(file_path)
            .await;

        let hot_stream = compressed_stream_from_disk.decompress();
        *writable_compressed_stream = Some(compressed_stream_from_disk);
        drop(writable_compressed_stream);

        hot_stream.hotstream_transmit_agg_chart(req, &tx).await;
        *writable_hot_stream = Some(hot_stream);
    }
}

impl From<TimePartitionFileHeader> for TimePartition {
    fn from(value: TimePartitionFileHeader) -> Self {
        let hash_map = DashMap::new();
        for x in value.disk_streams {
            hash_map.insert(
                x.stream_id,
                Stream {
                    disk_header: x,
                    compressed_stream: RwLock::new(None),
                    hot_stream: RwLock::new(None),
                },
            );
        }
        Self {
            start_unix_s: value.start_unix_s,
            streams: hash_map,
            file_path: value.file_path,
        }
    }
}

async fn sum_datapoints(
    mut rx: Receiver<DownsampledDatapoint>,
    meta: ChartReqMetadata,
) -> Vec<Datapoint> {
    let mut buffer: Vec<DownsampledDatapoint> = Vec::with_capacity(meta.resolution);
    let mut chart_values: Vec<Option<f32>> = vec![None; meta.resolution];

    while rx.recv_many(&mut buffer, meta.resolution).await != 0 {
        for pt in &buffer {
            chart_values[pt.time_index] = match chart_values[pt.time_index] {
                None => Some(pt.value),
                Some(x) => Some(pt.value + x),
            };
        }
        buffer.clear();
    }

    let datapoints = chart_values
        .into_iter()
        .enumerate()
        .filter_map(|(idx, value)| match value {
            Some(x) => Some(Datapoint {
                unix_s: meta.start_unix_s + (meta.step_s as usize * idx) as i64,
                value: x,
            }),
            None => None,
        })
        .collect();

    return datapoints;
}

async fn time_partition_transmit_stream(
    stream_id: u64,
    time_partition: Arc<TimePartition>,
    meta: ChartReqMetadata,
    tx: Sender<DownsampledDatapoint>,
) {
    let Some(stream) = time_partition.streams.get(&stream_id) else {
        return;
    };

    stream
        .stream_transmit_agg_chart(meta, &tx, &time_partition.file_path)
        .await;
}

async fn time_partition_get_agg_chart(
    time_partition: Arc<TimePartition>,
    req: Arc<AggChartRequest>,
    agg: Aggregation,
) -> Vec<Datapoint> {
    let meta = ChartReqMetadata::from_agg_chart_req(&req);

    let (tx, rx) = channel(meta.resolution);
    let coros = req
        .stream_ids
        .iter()
        .map(|x| time_partition_transmit_stream(*x, time_partition.clone(), meta, tx.clone()));

    let joinset = JoinSet::from_iter(coros);
    tokio::spawn(joinset.join_all());

    match agg {
        Aggregation::Sum => sum_datapoints(rx, meta).await,
        Aggregation::Mean => todo!(),
        Aggregation::Mode => todo!(),
        Aggregation::Median => todo!(),
        Aggregation::Min => todo!(),
        Aggregation::Max => todo!(),
    }
}

impl Storage {
    fn get_partitions_in_range(
        &self,
        start_unix_s: i64,
        stop_unix_s: i64,
    ) -> Vec<Arc<TimePartition>> {
        let mut partition_end = Utc::now().timestamp();
        let mut partitions_in_range = Vec::new();
        for partition in &self.partitions {
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
        let time_partitions = self.get_partitions_in_range(req.start_unix_s, req.stop_unix_s);
        let arc_req = Arc::new(req);
        let datapoint_jobs = time_partitions
            .into_iter()
            .map(|x| time_partition_get_agg_chart(x, arc_req.clone(), agg));
        let datapoints_nested = JoinSet::from_iter(datapoint_jobs).join_all().await;
        let datapoints_flattened = datapoints_nested.into_iter().flatten().collect();
        return datapoints_flattened;
    }
    pub fn new(config: StorageConfig) -> Self {
        todo!()
    }
}
