use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;
use arroyo_datastream::WindowType;
use arroyo_types::Watermark;

/// 表示一个窗口的特征
pub trait Window: Debug + Send + Sync + 'static {
    /// 获取窗口的开始时间
    fn start(&self) -> SystemTime;

    /// 获取窗口的结束时间
    fn end(&self) -> SystemTime;

    /// 获取窗口的最大允许延迟
    fn max_lateness(&self) -> Duration;

    /// 检查窗口是否已经过期（基于给定的水印）
    fn is_expired(&self, watermark: &Watermark) -> bool {
        match watermark {
            Watermark::EventTime(timestamp) => {
                self.end() + self.max_lateness() <= *timestamp
            }
            Watermark::Idle => false,
        }
    }

    /// 将窗口转换为 Any 类型，用于类型转换
    fn as_any(&self) -> &dyn std::any::Any;

    /// 从会话窗口创建窗口
    fn from_session_window(session_window: SessionWindow) -> Self where Self: Sized {
        panic!("Not implemented for this window type")
    }
}

/// 滚动窗口实现
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TumblingWindow {
    /// 窗口开始时间
    pub start: SystemTime,
    /// 窗口大小
    pub size: Duration,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl Window for TumblingWindow {
    fn start(&self) -> SystemTime {
        self.start
    }

    fn end(&self) -> SystemTime {
        self.start + self.size
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// 滑动窗口实现
#[derive(Debug, Clone)]
pub struct SlidingWindow {
    /// 窗口开始时间
    pub start: SystemTime,
    /// 窗口大小
    pub size: Duration,
    /// 滑动步长
    pub slide: Duration,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl Window for SlidingWindow {
    fn start(&self) -> SystemTime {
        self.start
    }

    fn end(&self) -> SystemTime {
        self.start + self.size
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// 会话窗口实现
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionWindow {
    /// 窗口开始时间
    pub start: SystemTime,
    /// 窗口结束时间
    pub end: SystemTime,
    /// 会话间隔
    pub gap: Duration,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl Window for SessionWindow {
    fn start(&self) -> SystemTime {
        self.start
    }

    fn end(&self) -> SystemTime {
        self.end
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn from_session_window(session_window: SessionWindow) -> Self {
        session_window
    }
}

/// 全局窗口实现
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GlobalWindow {
    /// 最大允许延迟
    pub max_lateness: Duration,
}

/// 动态窗口实现
///
/// 动态窗口允许根据数据特性动态调整窗口大小和边界
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DynamicWindow {
    /// 窗口开始时间
    pub start: SystemTime,
    /// 窗口结束时间
    pub end: SystemTime,
    /// 窗口 ID，用于唯一标识动态创建的窗口
    pub window_id: u64,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl Window for GlobalWindow {
    fn start(&self) -> SystemTime {
        // 全局窗口的开始时间是系统时间的最小值
        SystemTime::UNIX_EPOCH
    }

    fn end(&self) -> SystemTime {
        // 全局窗口的结束时间是系统时间的最大值
        // 使用一个较远但安全的未来时间，避免溢出
        SystemTime::UNIX_EPOCH + Duration::from_secs(60 * 60 * 24 * 365 * 100) // 100年
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Window for DynamicWindow {
    fn start(&self) -> SystemTime {
        self.start
    }

    fn end(&self) -> SystemTime {
        self.end
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// 多维窗口实现
///
/// 多维窗口允许在多个维度上进行窗口划分和聚合，例如同时按时间和用户 ID 进行窗口划分
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiDimensionalWindow {
    /// 窗口开始时间
    pub start: SystemTime,
    /// 窗口结束时间
    pub end: SystemTime,
    /// 窗口维度键，用于标识窗口的多个维度
    pub dimension_keys: Vec<String>,
    /// 窗口维度值，与维度键对应
    pub dimension_values: Vec<String>,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl Window for MultiDimensionalWindow {
    fn start(&self) -> SystemTime {
        self.start
    }

    fn end(&self) -> SystemTime {
        self.end
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// 窗口分配器特征，负责将元素分配到窗口
pub trait WindowAssigner: Debug + Send + Sync + 'static {
    /// 窗口类型
    type WindowT: Window;

    /// 获取窗口类型
    fn window_type(&self) -> WindowType;

    /// 将元素分配到窗口
    fn assign_windows(&self, element: &RecordBatch, timestamp: SystemTime) -> Vec<Self::WindowT>;

    /// 获取最大允许延迟
    fn max_lateness(&self) -> Duration;
}

/// 触发器特征，决定何时计算窗口结果
pub trait Trigger: Debug + Send + Sync + 'static {
    /// 窗口类型
    type WindowT: Window;

    /// 当元素添加到窗口时触发
    fn on_element(&mut self, element: &RecordBatch, window: &Self::WindowT, timestamp: SystemTime) -> bool;

    /// 当处理时间计时器触发时
    fn on_processing_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool;

    /// 当事件时间计时器触发时
    fn on_event_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool;

    /// 当窗口清除时触发
    fn on_clear(&mut self, window: &Self::WindowT);
}
