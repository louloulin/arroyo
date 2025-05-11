use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::SystemTime;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// 记录元数据
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct RecordMetadata {
    /// Topic 名称
    pub topic: String,
    /// 分区 ID
    pub partition: u32,
    /// 偏移量
    pub offset: u64,
    /// 水印时间
    pub watermark: Option<SystemTime>,
    /// 事件时间
    pub event_time: Option<SystemTime>,
}

impl RecordMetadata {
    /// 创建新的记录元数据
    pub fn new(topic: String, partition: u32, offset: u64) -> Self {
        Self {
            topic,
            partition,
            offset,
            watermark: None,
            event_time: None,
        }
    }

    /// 设置水印时间
    pub fn with_watermark(mut self, watermark: SystemTime) -> Self {
        self.watermark = Some(watermark);
        self
    }

    /// 设置事件时间
    pub fn with_event_time(mut self, event_time: SystemTime) -> Self {
        self.event_time = Some(event_time);
        self
    }
}

/// 统一记录类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct Record<T> {
    /// 记录键
    pub key: Option<Vec<u8>>,
    /// 记录值
    pub value: T,
    /// 记录头部
    pub headers: HashMap<String, Vec<u8>>,
    /// 时间戳
    pub timestamp: SystemTime,
    /// 元数据
    pub metadata: RecordMetadata,
}

impl<T> Record<T> {
    /// 创建新的记录
    pub fn new(value: T, timestamp: SystemTime, metadata: RecordMetadata) -> Self {
        Self {
            key: None,
            value,
            headers: HashMap::new(),
            timestamp,
            metadata,
        }
    }

    /// 设置记录键
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    /// 添加头部
    pub fn with_header(mut self, key: String, value: Vec<u8>) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// 添加多个头部
    pub fn with_headers(mut self, headers: HashMap<String, Vec<u8>>) -> Self {
        self.headers.extend(headers);
        self
    }

    /// 获取记录键
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// 获取记录值的引用
    pub fn value(&self) -> &T {
        &self.value
    }

    /// 获取记录值（消耗记录）
    pub fn into_value(self) -> T {
        self.value
    }

    /// 获取头部值
    pub fn header(&self, key: &str) -> Option<&[u8]> {
        self.headers.get(key).map(|v| v.as_slice())
    }

    /// 获取时间戳
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// 获取元数据
    pub fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    /// 映射记录值
    pub fn map<U, F>(self, f: F) -> Record<U>
    where
        F: FnOnce(T) -> U,
    {
        Record {
            key: self.key,
            value: f(self.value),
            headers: self.headers,
            timestamp: self.timestamp,
            metadata: self.metadata,
        }
    }
}

/// 统一流抽象
pub struct Stream<T> {
    // 内部实现将在后续添加
    _marker: PhantomData<T>,
}

impl<T> Stream<T> {
    /// 创建新的流
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// 转换实现将在后续添加
