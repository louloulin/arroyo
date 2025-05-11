use std::collections::HashMap;
use std::marker::PhantomData;

use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{Record, RecordMetadata};

/// 序列化格式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializationFormat {
    /// JSON 格式
    Json,
    /// Avro 格式
    Avro,
    /// Protobuf 格式
    Protobuf,
    /// 原始字节格式
    RawBytes,
}

/// 序列化器特征
pub trait Serializer<T> {
    /// 序列化记录
    fn serialize(&self, record: &Record<T>) -> Result<Vec<u8>>;

    /// 获取序列化格式
    fn format(&self) -> SerializationFormat;
}

/// 反序列化器特征
pub trait Deserializer<T> {
    /// 反序列化记录
    fn deserialize(&self, data: &[u8], metadata: RecordMetadata) -> Result<Record<T>>;

    /// 获取序列化格式
    fn format(&self) -> SerializationFormat;
}

/// JSON 序列化器
pub struct JsonSerializer<T> {
    _marker: PhantomData<T>,
}

impl<T> JsonSerializer<T> {
    /// 创建新的 JSON 序列化器
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: Serialize> Serializer<T> for JsonSerializer<T> {
    fn serialize(&self, record: &Record<T>) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(record)?;
        Ok(json)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
}

/// JSON 反序列化器
pub struct JsonDeserializer<T> {
    _marker: PhantomData<T>,
}

impl<T> JsonDeserializer<T> {
    /// 创建新的 JSON 反序列化器
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> Deserializer<T> for JsonDeserializer<T> {
    fn deserialize(&self, data: &[u8], metadata: RecordMetadata) -> Result<Record<T>> {
        let record: Record<T> = serde_json::from_slice(data)?;

        // 使用提供的元数据覆盖记录中的元数据
        let record = Record {
            key: record.key,
            value: record.value,
            headers: record.headers,
            timestamp: record.timestamp,
            metadata,
        };

        Ok(record)
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
}



/// 原始字节序列化器
pub struct RawBytesSerializer<T> {
    _marker: PhantomData<T>,
}

impl<T> RawBytesSerializer<T> {
    /// 创建新的原始字节序列化器
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Serializer<T> for RawBytesSerializer<T> {
    fn serialize(&self, record: &Record<T>) -> Result<Vec<u8>> {
        Ok(record.value.as_ref().to_vec())
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::RawBytes
    }
}

/// 原始字节反序列化器
pub struct RawBytesDeserializer<T> {
    _marker: PhantomData<T>,
}

impl<T> RawBytesDeserializer<T> {
    /// 创建新的原始字节反序列化器
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: From<Vec<u8>>> Deserializer<T> for RawBytesDeserializer<T> {
    fn deserialize(&self, data: &[u8], metadata: RecordMetadata) -> Result<Record<T>> {
        let value = T::from(data.to_vec());

        Ok(Record {
            key: None,
            value,
            headers: HashMap::new(),
            timestamp: std::time::SystemTime::now(),
            metadata,
        })
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::RawBytes
    }
}

/// 创建序列化器
pub fn create_serializer<T>(format: SerializationFormat) -> Result<Box<dyn Serializer<T>>>
where
    T: Serialize + AsRef<[u8]> + 'static,
{
    match format {
        SerializationFormat::Json => Ok(Box::new(JsonSerializer::new())),
        SerializationFormat::RawBytes => Ok(Box::new(RawBytesSerializer::new())),
        SerializationFormat::Avro => bail!("Avro serialization not implemented yet"),
        SerializationFormat::Protobuf => bail!("Protobuf serialization not implemented yet"),
    }
}

/// 创建反序列化器
pub fn create_deserializer<T>(format: SerializationFormat) -> Result<Box<dyn Deserializer<T>>>
where
    T: DeserializeOwned + 'static,
{
    match format {
        SerializationFormat::Json => Ok(Box::new(JsonDeserializer::new())),
        SerializationFormat::RawBytes => {
            bail!("RawBytes deserialization requires T: From<Vec<u8>>, use create_raw_bytes_deserializer instead")
        }
        SerializationFormat::Avro => bail!("Avro deserialization not implemented yet"),
        SerializationFormat::Protobuf => bail!("Protobuf deserialization not implemented yet"),
    }
}

/// 创建原始字节反序列化器
pub fn create_raw_bytes_deserializer<T>(format: SerializationFormat) -> Result<Box<dyn Deserializer<T>>>
where
    T: From<Vec<u8>> + 'static,
{
    match format {
        SerializationFormat::RawBytes => Ok(Box::new(RawBytesDeserializer::new())),
        _ => bail!("Only RawBytes format is supported by this function"),
    }
}
