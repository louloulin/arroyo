#[cfg(test)]
mod tests {
    use crate::topic::export::TopicExport;
    use arroyo_rpc::api_types::topics::TopicInfo;
    use tempfile::tempdir;

    #[test]
    fn test_topic_export_json() {
        // 创建测试数据
        let topics = vec![
            TopicInfo {
                name: "test-topic-1".to_string(),
                partitions: 3,
                replication_factor: 2,
                retention_ms: Some(86400000),
                retention_bytes: None,
                cleanup_policy: "delete".to_string(),
                max_message_bytes: None,
                description: Some("Test topic 1".to_string()),
                created_at: 1625097600,
                updated_at: 1625097600,
            },
            TopicInfo {
                name: "test-topic-2".to_string(),
                partitions: 1,
                replication_factor: 1,
                retention_ms: Some(172800000),
                retention_bytes: Some(1073741824),
                cleanup_policy: "compact".to_string(),
                max_message_bytes: Some(1048576),
                description: Some("Test topic 2".to_string()),
                created_at: 1625097700,
                updated_at: 1625097700,
            },
        ];

        // 创建导出对象
        let export = TopicExport::from_topic_infos(&topics);

        // 验证导出对象
        assert_eq!(export.topics.len(), 2);
        assert_eq!(export.topics[0].name, "test-topic-1");
        assert_eq!(export.topics[1].name, "test-topic-2");

        // 导出到 JSON 字符串
        let json = export.to_json_string().unwrap();

        // 验证 JSON 字符串
        assert!(json.contains("test-topic-1"));
        assert!(json.contains("test-topic-2"));

        // 从 JSON 字符串导入
        let imported = TopicExport::from_json_string(&json).unwrap();

        // 验证导入对象
        assert_eq!(imported.topics.len(), 2);
        assert_eq!(imported.topics[0].name, "test-topic-1");
        assert_eq!(imported.topics[1].name, "test-topic-2");
    }

    #[test]
    fn test_topic_export_yaml() {
        // 创建测试数据
        let topics = vec![
            TopicInfo {
                name: "test-topic-1".to_string(),
                partitions: 3,
                replication_factor: 2,
                retention_ms: Some(86400000),
                retention_bytes: None,
                cleanup_policy: "delete".to_string(),
                max_message_bytes: None,
                description: Some("Test topic 1".to_string()),
                created_at: 1625097600,
                updated_at: 1625097600,
            },
        ];

        // 创建导出对象
        let export = TopicExport::from_topic_infos(&topics);

        // 导出到 YAML 字符串
        let yaml = export.to_yaml_string().unwrap();

        // 验证 YAML 字符串
        assert!(yaml.contains("test-topic-1"));

        // 从 YAML 字符串导入
        let imported = TopicExport::from_yaml_string(&yaml).unwrap();

        // 验证导入对象
        assert_eq!(imported.topics.len(), 1);
        assert_eq!(imported.topics[0].name, "test-topic-1");
    }

    #[test]
    fn test_topic_export_file() {
        // 创建临时目录
        let dir = tempdir().unwrap();
        let json_path = dir.path().join("topics.json");
        let yaml_path = dir.path().join("topics.yaml");

        // 创建测试数据
        let topics = vec![
            TopicInfo {
                name: "test-topic-1".to_string(),
                partitions: 3,
                replication_factor: 2,
                retention_ms: Some(86400000),
                retention_bytes: None,
                cleanup_policy: "delete".to_string(),
                max_message_bytes: None,
                description: Some("Test topic 1".to_string()),
                created_at: 1625097600,
                updated_at: 1625097600,
            },
        ];

        // 创建导出对象
        let export = TopicExport::from_topic_infos(&topics);

        // 导出到 JSON 文件
        export.export_to_file(&json_path).unwrap();

        // 从 JSON 文件导入
        let imported_json = TopicExport::import_from_file(&json_path).unwrap();

        // 验证导入对象
        assert_eq!(imported_json.topics.len(), 1);
        assert_eq!(imported_json.topics[0].name, "test-topic-1");

        // 导出到 YAML 文件
        export.export_to_yaml_file(&yaml_path).unwrap();

        // 从 YAML 文件导入
        let imported_yaml = TopicExport::import_from_yaml_file(&yaml_path).unwrap();

        // 验证导入对象
        assert_eq!(imported_yaml.topics.len(), 1);
        assert_eq!(imported_yaml.topics[0].name, "test-topic-1");
    }
}
