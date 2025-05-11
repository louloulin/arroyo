#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arroyo_operator::context::{Collector, OperatorContext};
    use arroyo_operator::operator::ArrowOperator;
    use arroyo_types::ArroyoSchema;
    use datafusion::physical_plan::empty::EmptyExec;

    use crate::arrow::count_aggregating_window::CountAggregatingWindowFunc;
    use crate::arrow::global_window::{GlobalWindowFunc, TriggerType};

    struct TestCollector {
        pub collected: Vec<RecordBatch>,
    }

    #[async_trait::async_trait]
    impl Collector for TestCollector {
        async fn collect(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
            self.collected.push(batch);
            Ok(())
        }
    }

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["a", "b", "c", "d", "e"]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    fn create_arroyo_schema() -> Arc<ArroyoSchema> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        Arc::new(ArroyoSchema {
            schema: Arc::new(schema),
            routing_keys: Some(vec!["id".to_string()]),
            timestamp_field: None,
            watermark_field: None,
        })
    }

    #[tokio::test]
    async fn test_count_window() {
        // Create the operator
        let input_schema = create_arroyo_schema();
        let aggregation_plan = Arc::new(EmptyExec::new(false, input_schema.schema.clone()));
        let mut operator = CountAggregatingWindowFunc::<u64>::new(3, aggregation_plan, input_schema);

        // Create a test batch
        let batch = create_test_batch();

        // Create a collector
        let mut collector = TestCollector {
            collected: Vec::new(),
        };

        // Create a context
        let mut ctx = OperatorContext::default();

        // Process the batch
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Process another batch to trigger the window
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Process a third batch to trigger the window
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Check that we collected a batch
        assert_eq!(collector.collected.len(), 1);
    }

    #[tokio::test]
    async fn test_global_window_count_trigger() {
        // Create the operator
        let input_schema = create_arroyo_schema();
        let aggregation_plan = Arc::new(EmptyExec::new(false, input_schema.schema.clone()));
        let mut operator = GlobalWindowFunc::<u64>::new(
            TriggerType::Count { size: 3 },
            aggregation_plan,
            input_schema,
        );

        // Create a test batch
        let batch = create_test_batch();

        // Create a collector
        let mut collector = TestCollector {
            collected: Vec::new(),
        };

        // Create a context
        let mut ctx = OperatorContext::default();

        // Process the batch
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Process another batch
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Process a third batch to trigger the window
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Check that we collected a batch
        assert_eq!(collector.collected.len(), 1);
    }

    #[tokio::test]
    async fn test_global_window_time_trigger() {
        // Create the operator
        let input_schema = create_arroyo_schema();
        let aggregation_plan = Arc::new(EmptyExec::new(false, input_schema.schema.clone()));
        let mut operator = GlobalWindowFunc::<u64>::new(
            TriggerType::ProcessingTime {
                interval: Duration::from_millis(100),
            },
            aggregation_plan,
            input_schema,
        );

        // Create a test batch
        let batch = create_test_batch();

        // Create a collector
        let mut collector = TestCollector {
            collected: Vec::new(),
        };

        // Create a context
        let mut ctx = OperatorContext::default();

        // Process the batch
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Wait for the trigger interval
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Process another batch to trigger the window
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Check that we collected a batch
        assert_eq!(collector.collected.len(), 1);
    }

    #[tokio::test]
    async fn test_global_window_watermark_trigger() {
        // Create the operator
        let input_schema = create_arroyo_schema();
        let aggregation_plan = Arc::new(EmptyExec::new(false, input_schema.schema.clone()));
        let mut operator = GlobalWindowFunc::<u64>::new(
            TriggerType::Watermark,
            aggregation_plan,
            input_schema,
        );

        // Create a test batch
        let batch = create_test_batch();

        // Create a collector
        let mut collector = TestCollector {
            collected: Vec::new(),
        };

        // Create a context with a watermark
        let mut ctx = OperatorContext::default();
        ctx.set_last_present_watermark(SystemTime::now());

        // Process the batch
        operator
            .process_batch(batch.clone(), &mut ctx, &mut collector)
            .await;

        // Check that we collected a batch
        assert_eq!(collector.collected.len(), 1);
    }
}
