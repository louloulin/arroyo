use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;
use arroyo_datastream::WindowType;

use super::window_assigner::{
    DynamicWindow, GlobalWindow, MultiDimensionalWindow, SessionWindow, SlidingWindow, TumblingWindow, Window, WindowAssigner,
};

/// 滚动窗口分配器
#[derive(Debug, Clone)]
pub struct TumblingWindowAssigner {
    /// 窗口大小
    pub size: Duration,
    /// 偏移量
    pub offset: Duration,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl TumblingWindowAssigner {
    /// 创建新的滚动窗口分配器
    pub fn new(size: Duration, offset: Duration, max_lateness: Duration) -> Self {
        Self {
            size,
            offset,
            max_lateness,
        }
    }

    /// 计算窗口开始时间
    fn get_window_start_time(&self, timestamp: SystemTime) -> SystemTime {
        // 计算从 UNIX_EPOCH 开始的时间
        let duration_since_epoch = timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));

        // 应用偏移量
        let offset_duration = if duration_since_epoch >= self.offset {
            duration_since_epoch - self.offset
        } else {
            Duration::from_secs(0)
        };

        // 计算窗口开始时间
        let window_start_time = offset_duration.as_nanos() / self.size.as_nanos() * self.size.as_nanos();
        let window_start_duration = Duration::from_nanos(window_start_time as u64);

        // 加回偏移量
        SystemTime::UNIX_EPOCH + window_start_duration + self.offset
    }
}

impl WindowAssigner for TumblingWindowAssigner {
    type WindowT = TumblingWindow;

    fn window_type(&self) -> WindowType {
        WindowType::Tumbling { width: self.size }
    }

    fn assign_windows(&self, _element: &RecordBatch, timestamp: SystemTime) -> Vec<Self::WindowT> {
        let window_start = self.get_window_start_time(timestamp);

        vec![TumblingWindow {
            start: window_start,
            size: self.size,
            max_lateness: self.max_lateness,
        }]
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}

/// 滑动窗口分配器
#[derive(Debug, Clone)]
pub struct SlidingWindowAssigner {
    /// 窗口大小
    pub size: Duration,
    /// 滑动步长
    pub slide: Duration,
    /// 偏移量
    pub offset: Duration,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl SlidingWindowAssigner {
    /// 创建新的滑动窗口分配器
    pub fn new(size: Duration, slide: Duration, offset: Duration, max_lateness: Duration) -> Self {
        Self {
            size,
            slide,
            offset,
            max_lateness,
        }
    }
}

impl WindowAssigner for SlidingWindowAssigner {
    type WindowT = SlidingWindow;

    fn window_type(&self) -> WindowType {
        WindowType::Sliding {
            width: self.size,
            slide: self.slide,
        }
    }

    fn assign_windows(&self, _element: &RecordBatch, timestamp: SystemTime) -> Vec<Self::WindowT> {
        let mut windows = Vec::new();

        // 计算从 UNIX_EPOCH 开始的时间
        let duration_since_epoch = timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));

        // 应用偏移量
        let offset_duration = if duration_since_epoch >= self.offset {
            duration_since_epoch - self.offset
        } else {
            Duration::from_secs(0)
        };

        // 计算第一个窗口的结束时间
        let window_end_time = (offset_duration.as_nanos() / self.slide.as_nanos() + 1) * self.slide.as_nanos();
        let window_end = SystemTime::UNIX_EPOCH + Duration::from_nanos(window_end_time as u64) + self.offset;

        // 计算所有包含当前元素的窗口
        let mut window_start = window_end - self.size;

        // 添加所有包含当前元素的窗口
        while window_start <= timestamp && window_start + self.size > timestamp {
            windows.push(SlidingWindow {
                start: window_start,
                size: self.size,
                slide: self.slide,
                max_lateness: self.max_lateness,
            });

            window_start = window_start - self.slide;
        }

        windows
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}

/// 会话窗口分配器
#[derive(Debug, Clone)]
pub struct SessionWindowAssigner {
    /// 会话间隔
    pub gap: Duration,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl SessionWindowAssigner {
    /// 创建新的会话窗口分配器
    pub fn new(gap: Duration, max_lateness: Duration) -> Self {
        Self { gap, max_lateness }
    }
}

impl WindowAssigner for SessionWindowAssigner {
    type WindowT = SessionWindow;

    fn window_type(&self) -> WindowType {
        WindowType::Session { gap: self.gap }
    }

    fn assign_windows(&self, _element: &RecordBatch, timestamp: SystemTime) -> Vec<Self::WindowT> {
        vec![SessionWindow {
            start: timestamp,
            end: timestamp + self.gap,
            gap: self.gap,
            max_lateness: self.max_lateness,
        }]
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}

/// 全局窗口分配器
#[derive(Debug, Clone)]
pub struct GlobalWindowAssigner {
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl GlobalWindowAssigner {
    /// 创建新的全局窗口分配器
    pub fn new(max_lateness: Duration) -> Self {
        Self { max_lateness }
    }
}

impl WindowAssigner for GlobalWindowAssigner {
    type WindowT = GlobalWindow;

    fn window_type(&self) -> WindowType {
        WindowType::Global
    }

    fn assign_windows(&self, _element: &RecordBatch, _timestamp: SystemTime) -> Vec<Self::WindowT> {
        vec![GlobalWindow {
            max_lateness: self.max_lateness,
        }]
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}

/// 动态窗口分配器
///
/// 动态窗口分配器允许根据数据特性动态创建窗口
pub struct DynamicWindowAssigner {
    /// 窗口策略函数，用于确定窗口的开始和结束时间
    /// 输入为元素的时间戳，输出为窗口的开始和结束时间
    pub window_strategy: Box<dyn Fn(SystemTime) -> (SystemTime, SystemTime) + Send + Sync>,
    /// 最大允许延迟
    pub max_lateness: Duration,
    /// 下一个窗口 ID
    next_window_id: std::sync::atomic::AtomicU64,
}

impl DynamicWindowAssigner {
    /// 创建新的动态窗口分配器
    pub fn new<F>(window_strategy: F, max_lateness: Duration) -> Self
    where
        F: Fn(SystemTime) -> (SystemTime, SystemTime) + Send + Sync + 'static,
    {
        Self {
            window_strategy: Box::new(window_strategy),
            max_lateness,
            next_window_id: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 获取下一个窗口 ID
    fn next_window_id(&self) -> u64 {
        self.next_window_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

impl Debug for DynamicWindowAssigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicWindowAssigner")
            .field("max_lateness", &self.max_lateness)
            .field("next_window_id", &self.next_window_id)
            .field("window_strategy", &"<function>")
            .finish()
    }
}

impl Clone for DynamicWindowAssigner {
    fn clone(&self) -> Self {
        // 注意：这里我们无法真正克隆函数，所以创建一个新的分配器
        // 实际应用中，可能需要更复杂的克隆策略
        Self {
            // 创建一个简单的默认策略函数
            window_strategy: Box::new(move |timestamp| {
                // 默认策略：创建一个 10 秒的窗口
                (timestamp, timestamp + Duration::from_secs(10))
            }),
            max_lateness: self.max_lateness,
            next_window_id: std::sync::atomic::AtomicU64::new(
                self.next_window_id.load(std::sync::atomic::Ordering::SeqCst)
            ),
        }
    }
}

impl WindowAssigner for DynamicWindowAssigner {
    type WindowT = DynamicWindow;

    fn window_type(&self) -> WindowType {
        WindowType::Dynamic
    }

    fn assign_windows(&self, _element: &RecordBatch, timestamp: SystemTime) -> Vec<Self::WindowT> {
        // 使用窗口策略函数确定窗口的开始和结束时间
        let (start, end) = (self.window_strategy)(timestamp);

        vec![DynamicWindow {
            start,
            end,
            window_id: self.next_window_id(),
            max_lateness: self.max_lateness,
        }]
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}

/// 多维窗口分配器
///
/// 多维窗口分配器允许在多个维度上进行窗口划分和聚合，例如同时按时间和用户 ID 进行窗口划分
pub struct MultiDimensionalWindowAssigner {
    /// 时间窗口策略，用于确定窗口的时间维度
    pub time_window_strategy: Box<dyn Fn(SystemTime) -> (SystemTime, SystemTime) + Send + Sync>,
    /// 维度提取函数，用于从记录批次中提取维度键和值
    pub dimension_extractor: Box<dyn Fn(&RecordBatch) -> Vec<(String, String)> + Send + Sync>,
    /// 最大允许延迟
    pub max_lateness: Duration,
}

impl MultiDimensionalWindowAssigner {
    /// 创建新的多维窗口分配器
    pub fn new<F, E>(time_window_strategy: F, dimension_extractor: E, max_lateness: Duration) -> Self
    where
        F: Fn(SystemTime) -> (SystemTime, SystemTime) + Send + Sync + 'static,
        E: Fn(&RecordBatch) -> Vec<(String, String)> + Send + Sync + 'static,
    {
        Self {
            time_window_strategy: Box::new(time_window_strategy),
            dimension_extractor: Box::new(dimension_extractor),
            max_lateness,
        }
    }
}

impl Debug for MultiDimensionalWindowAssigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiDimensionalWindowAssigner")
            .field("max_lateness", &self.max_lateness)
            .field("time_window_strategy", &"<function>")
            .field("dimension_extractor", &"<function>")
            .finish()
    }
}

impl Clone for MultiDimensionalWindowAssigner {
    fn clone(&self) -> Self {
        // 注意：这里我们无法真正克隆函数，所以创建一个新的分配器
        // 实际应用中，可能需要更复杂的克隆策略
        Self {
            // 创建一个简单的默认时间窗口策略函数
            time_window_strategy: Box::new(move |timestamp| {
                // 默认策略：创建一个 10 秒的窗口
                (timestamp, timestamp + Duration::from_secs(10))
            }),
            // 创建一个简单的默认维度提取函数
            dimension_extractor: Box::new(|_| {
                // 默认策略：不提取任何维度
                Vec::new()
            }),
            max_lateness: self.max_lateness,
        }
    }
}

impl WindowAssigner for MultiDimensionalWindowAssigner {
    type WindowT = MultiDimensionalWindow;

    fn window_type(&self) -> WindowType {
        WindowType::MultiDimensional
    }

    fn assign_windows(&self, element: &RecordBatch, timestamp: SystemTime) -> Vec<Self::WindowT> {
        // 使用时间窗口策略函数确定窗口的时间维度
        let (start, end) = (self.time_window_strategy)(timestamp);

        // 使用维度提取函数确定窗口的其他维度
        let dimensions = (self.dimension_extractor)(element);

        // 如果没有提取到维度，则创建一个默认的多维窗口
        if dimensions.is_empty() {
            return vec![MultiDimensionalWindow {
                start,
                end,
                dimension_keys: Vec::new(),
                dimension_values: Vec::new(),
                max_lateness: self.max_lateness,
            }];
        }

        // 提取维度键和值
        let mut dimension_keys = Vec::new();
        let mut dimension_values = Vec::new();
        for (key, value) in dimensions {
            dimension_keys.push(key);
            dimension_values.push(value);
        }

        vec![MultiDimensionalWindow {
            start,
            end,
            dimension_keys,
            dimension_values,
            max_lateness: self.max_lateness,
        }]
    }

    fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}
