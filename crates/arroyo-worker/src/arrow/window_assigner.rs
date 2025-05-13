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
}

/// 会话窗口实现
#[derive(Debug, Clone)]
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
}

/// 全局窗口实现
#[derive(Debug, Clone)]
pub struct GlobalWindow {
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
        // 这里使用一个非常远的未来时间
        SystemTime::UNIX_EPOCH + Duration::from_secs(u64::MAX)
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
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
