use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// 压缩算法类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    /// 不压缩
    None,
    /// GZIP 压缩
    Gzip,
    /// LZ4 压缩
    Lz4,
    /// Snappy 压缩
    Snappy,
    /// ZSTD 压缩
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Zstd => write!(f, "zstd"),
        }
    }
}

impl FromStr for CompressionType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(CompressionType::None),
            "gzip" => Ok(CompressionType::Gzip),
            "lz4" => Ok(CompressionType::Lz4),
            "snappy" => Ok(CompressionType::Snappy),
            "zstd" => Ok(CompressionType::Zstd),
            _ => Err(Error::InvalidCompressionType(s.to_string())),
        }
    }
}

/// 压缩器特征
pub trait Compressor: Send + Sync {
    /// 压缩数据
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
}

/// 解压缩器特征
pub trait Decompressor: Send + Sync {
    /// 解压缩数据
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
}

/// 不压缩
pub struct NoneCompressor;

impl Compressor for NoneCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

impl Decompressor for NoneCompressor {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// GZIP 压缩
pub struct GzipCompressor {
    level: flate2::Compression,
}

impl GzipCompressor {
    pub fn new(level: u32) -> Self {
        let level = match level {
            0 => flate2::Compression::none(),
            1 => flate2::Compression::fast(),
            2..=8 => flate2::Compression::new(level),
            _ => flate2::Compression::best(),
        };
        Self { level }
    }
}

impl Compressor for GzipCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), self.level);
        encoder.write_all(data)?;
        let compressed = encoder.finish()?;
        Ok(compressed)
    }
}

impl Decompressor for GzipCompressor {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }
}

/// LZ4 压缩
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }
}

impl Decompressor for Lz4Compressor {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data).map_err(|e| Error::DecompressionError(e.to_string()))
    }
}

/// Snappy 压缩
pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(snap::raw::Encoder::new().compress_vec(data)?)
    }
}

impl Decompressor for SnappyCompressor {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(snap::raw::Decoder::new().decompress_vec(data)?)
    }
}

/// ZSTD 压缩
pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(data, self.level).map_err(|e| Error::CompressionError(format!("{:?}", e)))
    }
}

impl Decompressor for ZstdCompressor {
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::decode_all(data).map_err(|e| Error::CompressionError(format!("{:?}", e)))
    }
}

/// 创建压缩器
pub fn create_compressor(compression_type: CompressionType) -> Arc<dyn Compressor + Send + Sync> {
    match compression_type {
        CompressionType::None => Arc::new(NoneCompressor),
        CompressionType::Gzip => Arc::new(GzipCompressor::new(6)), // 默认压缩级别
        CompressionType::Lz4 => Arc::new(Lz4Compressor),
        CompressionType::Snappy => Arc::new(SnappyCompressor),
        CompressionType::Zstd => Arc::new(ZstdCompressor::new(3)), // 默认压缩级别
    }
}

/// 创建解压缩器
pub fn create_decompressor(compression_type: CompressionType) -> Arc<dyn Decompressor + Send + Sync> {
    match compression_type {
        CompressionType::None => Arc::new(NoneCompressor),
        CompressionType::Gzip => Arc::new(GzipCompressor::new(0)), // 解压缩不需要压缩级别
        CompressionType::Lz4 => Arc::new(Lz4Compressor),
        CompressionType::Snappy => Arc::new(SnappyCompressor),
        CompressionType::Zstd => Arc::new(ZstdCompressor::new(0)), // 解压缩不需要压缩级别
    }
}
