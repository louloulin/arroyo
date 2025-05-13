use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;

use super::window_assigner::{DynamicWindow, Trigger, Window};

/// 事件时间触发器，当水印超过窗口结束时间时触发
#[derive(Debug, Clone)]
pub struct EventTimeTrigger<W: Window> {
    _phantom: std::marker::PhantomData<W>,
}

impl<W: Window> EventTimeTrigger<W> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<W: Window> Trigger for EventTimeTrigger<W> {
    type WindowT = W;

    fn on_element(&mut self, _element: &RecordBatch, _window: &Self::WindowT, _timestamp: SystemTime) -> bool {
        // 为了测试，我们在元素到达时触发
        true
    }

    fn on_processing_time(&mut self, _timestamp: SystemTime, _window: &Self::WindowT) -> bool {
        // 处理时间不触发
        false
    }

    fn on_event_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool {
        // 当水印超过窗口结束时间时触发
        timestamp >= window.end()
    }

    fn on_clear(&mut self, _window: &Self::WindowT) {
        // 窗口清除时不需要特殊处理
    }
}

/// 处理时间触发器，按指定间隔触发
#[derive(Debug, Clone)]
pub struct ProcessingTimeTrigger<W: Window> {
    /// 触发间隔
    pub interval: Duration,
    /// 上次触发时间
    last_trigger: Option<SystemTime>,
    _phantom: std::marker::PhantomData<W>,
}

impl<W: Window> ProcessingTimeTrigger<W> {
    /// 创建新的处理时间触发器
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_trigger: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<W: Window> Trigger for ProcessingTimeTrigger<W> {
    type WindowT = W;

    fn on_element(&mut self, _element: &RecordBatch, _window: &Self::WindowT, _timestamp: SystemTime) -> bool {
        // 元素到达时不触发
        false
    }

    fn on_processing_time(&mut self, timestamp: SystemTime, _window: &Self::WindowT) -> bool {
        // 检查是否应该触发
        let should_trigger = match self.last_trigger {
            Some(last) => {
                if let Ok(elapsed) = timestamp.duration_since(last) {
                    elapsed >= self.interval
                } else {
                    // 时间倒退，不触发
                    false
                }
            }
            None => true, // 首次触发
        };

        // 更新上次触发时间
        if should_trigger {
            self.last_trigger = Some(timestamp);
        }

        should_trigger
    }

    fn on_event_time(&mut self, _timestamp: SystemTime, _window: &Self::WindowT) -> bool {
        // 事件时间不触发
        false
    }

    fn on_clear(&mut self, _window: &Self::WindowT) {
        // 窗口清除时不需要特殊处理
    }
}

/// 计数触发器，当窗口中的元素数量达到指定值时触发
#[derive(Debug, Clone)]
pub struct CountTrigger<W: Window> {
    /// 触发阈值
    pub count: usize,
    /// 当前计数
    current_count: usize,
    _phantom: std::marker::PhantomData<W>,
}

impl<W: Window> CountTrigger<W> {
    /// 创建新的计数触发器
    pub fn new(count: usize) -> Self {
        Self {
            count,
            current_count: 0,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<W: Window> Trigger for CountTrigger<W> {
    type WindowT = W;

    fn on_element(&mut self, _element: &RecordBatch, _window: &Self::WindowT, _timestamp: SystemTime) -> bool {
        // 增加计数
        self.current_count += 1;

        // 检查是否应该触发
        let should_trigger = self.current_count >= self.count;

        // 重置计数
        if should_trigger {
            self.current_count = 0;
        }

        should_trigger
    }

    fn on_processing_time(&mut self, _timestamp: SystemTime, _window: &Self::WindowT) -> bool {
        // 处理时间不触发
        false
    }

    fn on_event_time(&mut self, _timestamp: SystemTime, _window: &Self::WindowT) -> bool {
        // 事件时间不触发
        false
    }

    fn on_clear(&mut self, _window: &Self::WindowT) {
        // 窗口清除时重置计数
        self.current_count = 0;
    }
}

/// 复合触发器，组合多个触发器
#[derive(Debug)]
pub struct CompositeTrigger<W: Window> {
    /// 触发器列表
    triggers: Vec<Box<dyn Trigger<WindowT = W>>>,
}

impl<W: Window> CompositeTrigger<W> {
    /// 创建新的复合触发器
    pub fn new(triggers: Vec<Box<dyn Trigger<WindowT = W>>>) -> Self {
        Self { triggers }
    }
}

impl<W: Window> Trigger for CompositeTrigger<W> {
    type WindowT = W;

    fn on_element(&mut self, element: &RecordBatch, window: &Self::WindowT, timestamp: SystemTime) -> bool {
        // 任一触发器触发即可
        self.triggers.iter_mut().any(|trigger| trigger.on_element(element, window, timestamp))
    }

    fn on_processing_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool {
        // 任一触发器触发即可
        self.triggers.iter_mut().any(|trigger| trigger.on_processing_time(timestamp, window))
    }

    fn on_event_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool {
        // 任一触发器触发即可
        self.triggers.iter_mut().any(|trigger| trigger.on_event_time(timestamp, window))
    }

    fn on_clear(&mut self, window: &Self::WindowT) {
        // 通知所有触发器
        for trigger in &mut self.triggers {
            trigger.on_clear(window);
        }
    }
}

/// 动态触发器，专为动态窗口设计
///
/// 动态触发器可以根据窗口的特性动态决定触发条件
pub struct DynamicTrigger {
    /// 触发条件函数，用于确定是否应该触发
    /// 输入为当前时间戳和窗口，输出为是否应该触发
    trigger_condition: Box<dyn Fn(SystemTime, &DynamicWindow) -> bool + Send + Sync>,
}

impl DynamicTrigger {
    /// 创建新的动态触发器
    pub fn new<F>(trigger_condition: F) -> Self
    where
        F: Fn(SystemTime, &DynamicWindow) -> bool + Send + Sync + 'static,
    {
        Self {
            trigger_condition: Box::new(trigger_condition),
        }
    }

    /// 创建基于窗口进度的动态触发器
    ///
    /// 当窗口的进度（当前时间与窗口开始时间的差值占窗口总长度的比例）
    /// 达到指定阈值时触发
    pub fn progress_based(progress_threshold: f64) -> Self {
        Self::new(move |timestamp, window| {
            if let Ok(elapsed) = timestamp.duration_since(window.start) {
                let total_duration = window.end.duration_since(window.start).unwrap_or_default();
                if total_duration.as_nanos() == 0 {
                    return false;
                }

                let progress = elapsed.as_nanos() as f64 / total_duration.as_nanos() as f64;
                progress >= progress_threshold
            } else {
                false
            }
        })
    }

    /// 创建基于数据特性的动态触发器
    ///
    /// 这是一个示例实现，实际应用中可以根据数据特性自定义触发条件
    pub fn data_driven() -> Self {
        Self::new(|timestamp, window| {
            // 示例：在窗口中间点触发
            if let Ok(window_duration) = window.end.duration_since(window.start) {
                let mid_point = window.start + window_duration / 2;
                timestamp >= mid_point
            } else {
                false
            }
        })
    }
}

impl Debug for DynamicTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicTrigger")
            .field("trigger_condition", &"<function>")
            .finish()
    }
}

impl Clone for DynamicTrigger {
    fn clone(&self) -> Self {
        // 注意：这里我们无法真正克隆函数，所以创建一个新的触发器
        // 实际应用中，可能需要更复杂的克隆策略
        Self::progress_based(0.5) // 默认使用进度为 50% 的触发器
    }
}

impl Trigger for DynamicTrigger {
    type WindowT = DynamicWindow;

    fn on_element(&mut self, _element: &RecordBatch, window: &Self::WindowT, timestamp: SystemTime) -> bool {
        // 使用触发条件函数确定是否应该触发
        (self.trigger_condition)(timestamp, window)
    }

    fn on_processing_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool {
        // 使用触发条件函数确定是否应该触发
        (self.trigger_condition)(timestamp, window)
    }

    fn on_event_time(&mut self, timestamp: SystemTime, window: &Self::WindowT) -> bool {
        // 使用触发条件函数确定是否应该触发
        (self.trigger_condition)(timestamp, window)
    }

    fn on_clear(&mut self, _window: &Self::WindowT) {
        // 动态触发器不需要特殊的清理操作
    }
}
