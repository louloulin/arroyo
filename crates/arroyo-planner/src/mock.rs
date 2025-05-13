// 这个文件包含了一些模拟的结构体和函数，用于在没有 arroyo-operator 特性时使用

use arroyo_rpc::api_types::connections::{ConnectionType, SourceField};
use arroyo_rpc::formats::{Format, Framing};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ConnectionSchema {
    pub format: Format,
    pub framing: Framing,
    pub fields: Vec<SourceField>,
    pub primary_keys: Vec<String>,
    pub inferred: Option<bool>,
}

impl ConnectionSchema {
    pub fn try_new(
        format: Format,
        _bad_data: String,
        framing: Framing,
        _schema_registry_subject: Option<String>,
        fields: Vec<SourceField>,
        _schema_registry_id: Option<i32>,
        inferred: Option<bool>,
        primary_keys: Vec<String>,
    ) -> Result<Self, String> {
        Ok(ConnectionSchema {
            format,
            framing,
            fields,
            primary_keys,
            inferred,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Connection {
    pub id: i64,
    pub name: String,
    pub connector: String,
    pub connection_type: ConnectionType,
    pub schema: ConnectionSchema,
    pub config: String,
    pub description: String,
    pub partition_fields: Option<Vec<String>>,
}

impl Connection {
    pub fn from_options(
        name: &str,
        _options: &HashMap<String, String>,
        _schema: Option<&ConnectionSchema>,
        _connection_profile: Option<&arroyo_rpc::api_types::connections::ConnectionProfile>,
    ) -> Result<Self, String> {
        Ok(Connection {
            id: 0,
            name: name.to_string(),
            connector: "mock".to_string(),
            connection_type: ConnectionType::Source,
            schema: ConnectionSchema {
                format: Format::Json(arroyo_rpc::formats::JsonFormat {
                    debezium: false,
                    flatten: false,
                }),
                framing: Framing::None,
                fields: vec![],
                primary_keys: vec![],
                inferred: Some(true),
            },
            config: "".to_string(),
            description: "".to_string(),
            partition_fields: None,
        })
    }

    pub fn into(self) -> crate::tables::ConnectorTable {
        crate::tables::ConnectorTable {
            name: self.name,
            connection_id: self.id,
            connection_type: self.connection_type,
            fields: self.schema.fields,
            partition_fields: self.partition_fields,
        }
    }
}
