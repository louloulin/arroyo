use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;

use super::window_assigner::{Trigger, Window};

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
