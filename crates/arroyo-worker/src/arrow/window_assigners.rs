use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;
use arroyo_datastream::WindowType;

use super::window_assigner::{
    GlobalWindow, SessionWindow, SlidingWindow, TumblingWindow, Window, WindowAssigner,
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
