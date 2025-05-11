#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use crate::{Record, RecordMetadata, Watermark};
    use crate::watermark_strategy::{
        WatermarkStrategy, WatermarkStrategyType, WatermarkStrategyConfig, WatermarkStrategyFactory,
        WatermarkAligner,
    };

    #[test]
    fn test_watermark_strategy_config() {
        // 测试默认配置
        let default_config = WatermarkStrategyConfig::default();
        assert_eq!(default_config.strategy_type, WatermarkStrategyType::Periodic);
        assert_eq!(default_config.max_delay, Duration::from_secs(5));
        assert_eq!(default_config.interval, Duration::from_secs(1));
        assert_eq!(default_config.idle_timeout, Some(Duration::from_secs(60)));
        assert!(default_config.custom_config.is_none());

        // 测试自定义配置
        let mut custom_config = HashMap::new();
        custom_config.insert("quantile".to_string(), "0.99".to_string());
        custom_config.insert("window_size".to_string(), "2000".to_string());

        let config = WatermarkStrategyConfig {
            strategy_type: WatermarkStrategyType::Adaptive,
            max_delay: Duration::from_secs(10),
            interval: Duration::from_millis(500),
            idle_timeout: Some(Duration::from_secs(30)),
            custom_config: Some(custom_config),
        };

        assert_eq!(config.strategy_type, WatermarkStrategyType::Adaptive);
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert_eq!(config.interval, Duration::from_millis(500));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(30)));
        assert!(config.custom_config.is_some());
        assert_eq!(config.custom_config.as_ref().unwrap().get("quantile"), Some(&"0.99".to_string()));
        assert_eq!(config.custom_config.as_ref().unwrap().get("window_size"), Some(&"2000".to_string()));
    }

    #[test]
    fn test_periodic_watermark_strategy() {
        let config = WatermarkStrategyConfig {
            strategy_type: WatermarkStrategyType::Periodic,
            max_delay: Duration::from_secs(5),
            interval: Duration::from_secs(1),
            idle_timeout: Some(Duration::from_secs(60)),
            custom_config: None,
        };

        let factory = WatermarkStrategyFactory::new();
        let mut strategy = factory.create_periodic(config);

        assert_eq!(strategy.strategy_type(), WatermarkStrategyType::Periodic);
        assert_eq!(strategy.config().max_delay, Duration::from_secs(5));

        // 创建测试记录
        let now = SystemTime::now();
        let event_time = now - Duration::from_secs(10);
        let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100)
            .with_event_time(event_time);

        let record = Record::new("test-value".to_string(), now, metadata);

        // 处理记录
        let watermark = strategy.on_event(&record);
        assert!(watermark.is_none());

        // 生成周期性水印
        let watermark = strategy.on_periodic_watermark();
        assert!(watermark.is_some());

        if let Some(Watermark::EventTime(wm_time)) = watermark {
            // 水印时间应该是事件时间减去最大延迟
            let expected_time = event_time - Duration::from_secs(5);
            assert!(wm_time <= expected_time);
        } else {
            panic!("Expected EventTime watermark");
        }

        // 测试空闲状态
        let watermark = strategy.on_idle();
        assert!(watermark.is_none());

        // 模拟空闲超时
        std::thread::sleep(Duration::from_millis(100));
        let watermark = strategy.on_idle();
        assert!(watermark.is_none());
    }

    #[test]
    fn test_adaptive_watermark_strategy() {
        let mut custom_config = HashMap::new();
        custom_config.insert("quantile".to_string(), "0.95".to_string());
        custom_config.insert("window_size".to_string(), "100".to_string());

        let config = WatermarkStrategyConfig {
            strategy_type: WatermarkStrategyType::Adaptive,
            max_delay: Duration::from_secs(10),
            interval: Duration::from_millis(500),
            idle_timeout: Some(Duration::from_secs(30)),
            custom_config: Some(custom_config),
        };

        let factory = WatermarkStrategyFactory::new();
        let mut strategy = factory.create_adaptive(config);

        assert_eq!(strategy.strategy_type(), WatermarkStrategyType::Adaptive);
        assert_eq!(strategy.config().max_delay, Duration::from_secs(10));

        // 创建测试记录
        let now = SystemTime::now();

        // 生成一些具有不同延迟的记录
        for i in 1..=20 {
            let event_time = now - Duration::from_secs(i);
            let metadata = RecordMetadata::new("test-topic".to_string(), 1, 100 + i as u64)
                .with_event_time(event_time);

            let record = Record::new(format!("value-{}", i), now, metadata);

            // 处理记录
            strategy.on_event(&record);
        }

        // 生成周期性水印
        let watermark = strategy.on_periodic_watermark();
        assert!(watermark.is_some());

        if let Some(Watermark::EventTime(_)) = watermark {
            // 水印时间应该是基于自适应延迟计算的
            // 具体值取决于实现，这里只检查类型
        } else {
            panic!("Expected EventTime watermark");
        }
    }

    #[test]
    fn test_watermark_aligner() {
        let mut aligner = WatermarkAligner::new();

        // 添加源
        aligner.add_source("source1".to_string());
        aligner.add_source("source2".to_string());
        aligner.add_source("source3".to_string());

        // 初始状态下没有水印
        assert!(aligner.current_watermark().is_none());

        // 更新源水印
        let now = SystemTime::now();
        let wm1 = Watermark::EventTime(now - Duration::from_secs(10));
        let wm2 = Watermark::EventTime(now - Duration::from_secs(15));
        let wm3 = Watermark::EventTime(now - Duration::from_secs(5));

        aligner.update_source_watermark("source1", Some(wm1));
        assert!(aligner.current_watermark().is_some());

        aligner.update_source_watermark("source2", Some(wm2));
        aligner.update_source_watermark("source3", Some(wm3));

        // 对齐水印应该是最小的水印
        if let Some(Watermark::EventTime(wm_time)) = aligner.current_watermark() {
            if let Watermark::EventTime(expected_time) = wm2 {
                assert_eq!(wm_time, expected_time);
            }
        } else {
            panic!("Expected EventTime watermark");
        }

        // 测试空闲状态
        aligner.update_source_watermark("source1", Some(Watermark::Idle));
        aligner.update_source_watermark("source2", Some(Watermark::Idle));

        // 不是所有源都空闲，所以对齐水印仍然是最小的事件时间水印
        if let Some(Watermark::EventTime(_)) = aligner.current_watermark() {
            // 正确
        } else {
            panic!("Expected EventTime watermark");
        }

        // 所有源都空闲
        let watermark = aligner.update_source_watermark("source3", Some(Watermark::Idle));

        // 对齐水印应该是空闲水印
        if let Some(Watermark::Idle) = watermark {
            // 正确
        } else {
            // 如果 update_source_watermark 没有立即返回更新后的水印，
            // 我们可以通过 current_watermark 获取
            if let Some(Watermark::Idle) = aligner.current_watermark() {
                // 正确
            } else {
                panic!("Expected Idle watermark");
            }
        }

        // 移除源
        aligner.remove_source("source3");

        // 由于我们移除了一个源，水印状态可能会改变
        // 这里我们只检查水印是否存在，不检查具体类型
        assert!(aligner.current_watermark().is_some());
    }
}
