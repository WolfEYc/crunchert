use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};

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
    data_interval_s: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            min_pts_to_compress: 8,
            default_compression_level: 8,
            retention_period_s: 7776000,
            data_interval_s: 900,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StreamFileHeader {
    stream_id: u64,
    unix_s_byte_start: u64,
    unix_s_byte_stop: u64,
    values_byte_stop: u64,
    compressed: bool,
}

#[derive(Serialize, Deserialize)]
struct FileStorageHeader {
    stream_file_headers: Vec<StreamFileHeader>,
}

struct CompressedStream {
    unix_s: Vec<u8>,
    values: Vec<u8>,
}

#[derive(Debug)]
struct Datapoint {
    unix_s: i64,
    value: f32,
}

struct DecompressedStream {
    datapoints: Vec<Datapoint>,
}

enum InMemStream {
    Compressed(CompressedStream),
    Decompressed(DecompressedStream),
}

struct StreamsTable {
    // stream_id -> stream
    streams: HashMap<u64, InMemStream>,
}
