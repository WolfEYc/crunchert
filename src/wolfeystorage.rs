use dashmap::DashMap;
use prost::UnknownEnumValue;
use std::sync::Arc;
use std::usize;
use std::{fmt::Debug, path::PathBuf, time::Duration};
use tokio::task::JoinSet;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;

use crate::wolfey_metrics::{AggChartRequest, Aggregation, NonAggChartRequest};

#[derive(Debug, Copy, Clone)]
struct Datapoint {
    unix_s: i64,
    value: f32,
}
struct DownsampledDatapoint {
    time_index: usize,
    value: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct CompressionOffset {
    offset: Duration,
    compression_level: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct StorageConfig {
    min_pts_to_compress: usize,
    default_compression_level: usize,
    retention_period_s: usize,
    data_frequency_s: usize,
    max_parallelism: usize,
    stream_cache_ttl_s: usize,
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

struct HotStream {
    datapoints: Vec<Datapoint>,
}

struct Stream {
    disk_header: DiskStreamFileHeader,
    hot_stream: RwLock<Option<HotStream>>,
}

struct TimePartition {
    start_unix_s: i64,
    file_path: PathBuf,
    streams: DashMap<u64, Stream>,
}

pub struct Storage {
    config: StorageConfig,
    partitions: Vec<TimePartition>,
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
    pub fn from_agg_chart_req(value: &AggChartRequest) -> Self {
        Self {
            start_unix_s: value.start_unix_s,
            stop_unix_s: value.stop_unix_s,
            step_s: value.step_s,
            resolution: resolution(value.start_unix_s, value.stop_unix_s, value.step_s),
        }
    }
    pub fn from_non_agg_chart_req(value: &NonAggChartRequest) -> Self {
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
            max_parallelism: num_cpus::get(),
            stream_cache_ttl_s: 900,
        }
    }
}

impl HotStream {
    pub async fn send_agg_chart(&self, req: ChartReqMetadata, tx: &Sender<DownsampledDatapoint>) {
        for pt in &self.datapoints {
            //TODO: only send downsampled pts, with time index
            let _ = tx
                .send(DownsampledDatapoint {
                    time_index: 1,
                    value: pt.value,
                })
                .await;
        }
    }
}

impl DiskStreamFileHeader {
    async fn get_hot_stream_from_disk(&self, file_path: &PathBuf) -> HotStream {
        todo!()
    }
}

impl Stream {
    pub async fn get_agg_chart(
        &self,
        req: ChartReqMetadata,
        tx: &Sender<DownsampledDatapoint>,
        file_path: &PathBuf,
    ) {
        let hot_stream_option = self.hot_stream.read().await;
        if let Some(ref x) = *hot_stream_option {
            x.send_agg_chart(req, &tx).await;
            return;
        }

        let mut writable_hot_stream = self.hot_stream.write().await;

        if let Some(ref x) = *writable_hot_stream {
            x.send_agg_chart(req, &tx).await;
            return;
        }

        let hot_stream_from_disk = self.disk_header.get_hot_stream_from_disk(file_path).await;
        hot_stream_from_disk.send_agg_chart(req, &tx).await;
        *writable_hot_stream = Some(hot_stream_from_disk);
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

async fn get_agg_chart_from_partition(
    stream_id: u64,
    time_partition: Arc<TimePartition>,
    meta: ChartReqMetadata,
    tx: Sender<DownsampledDatapoint>,
) {
    let Some(stream) = time_partition.streams.get(&stream_id) else {
        return;
    };

    stream
        .get_agg_chart(meta, &tx, &time_partition.file_path)
        .await;
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

async fn get_agg_chart_from_partition_bulk(
    time_partition: Arc<TimePartition>,
    req: AggChartRequest,
) -> Result<Vec<Datapoint>, UnknownEnumValue> {
    let meta = ChartReqMetadata::from_agg_chart_req(&req);

    let (tx, rx) = channel(meta.resolution);
    let coros = req
        .stream_ids
        .into_iter()
        .map(|x| get_agg_chart_from_partition(x, time_partition.clone(), meta, tx.clone()));

    let joinset = JoinSet::from_iter(coros);
    tokio::spawn(joinset.join_all());
    let aggregation = Aggregation::try_from(req.aggregation)?;

    return Ok(match aggregation {
        Aggregation::Sum => sum_datapoints(rx, meta).await,
        Aggregation::Mean => todo!(),
        Aggregation::Mode => todo!(),
        Aggregation::Median => todo!(),
        Aggregation::Min => todo!(),
        Aggregation::Max => todo!(),
    });
}
impl Storage {
    fn get_partitions_in_range(&self, start_unix_s: i64, stop_unix_s: i64) -> Vec<&TimePartition> {
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
            partitions_in_range.push(partition);
        }
        return partitions_in_range;
    }
}
