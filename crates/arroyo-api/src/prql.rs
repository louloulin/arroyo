use axum::{extract::State, Json};
use axum_extra::extract::WithRejection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::rest_utils::{bad_request, ApiError, ErrorResp};
use crate::rest::AppState;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PrqlConvertRequest {
    pub query: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PrqlConvertResponse {
    pub sql: String,
}

/// Convert PRQL to SQL
///
/// This endpoint converts a PRQL query to SQL.
#[utoipa::path(
    post,
    path = "/v1/prql/convert",
    request_body = PrqlConvertRequest,
    responses(
        (status = 200, description = "PRQL query converted to SQL", body = PrqlConvertResponse),
        (status = 400, description = "Bad request", body = ErrorResp),
        (status = 401, description = "Unauthorized", body = ErrorResp),
    ),
    security(
        ("bearer" = [])
    )
)]
pub async fn convert_prql(
    State(_state): State<AppState>,
    WithRejection(Json(request), _): WithRejection<Json<PrqlConvertRequest>, ApiError>,
) -> Result<Json<PrqlConvertResponse>, ErrorResp> {
    // Convert PRQL to SQL
    match arroyo_prql::prql_to_sql(&request.query) {
        Ok(sql) => Ok(Json(PrqlConvertResponse { sql })),
        Err(e) => Err(bad_request(format!("Failed to convert PRQL to SQL: {}", e))),
    }
}

/// Cache for PRQL to SQL conversions
pub struct PrqlCache {
    // TODO: Implement a proper cache with LRU or similar
    // For now, we'll just use a simple in-memory cache
}

impl PrqlCache {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get(&self, _query: &str) -> Option<String> {
        // TODO: Implement cache lookup
        None
    }

    pub fn put(&self, _query: &str, _sql: &str) {
        // TODO: Implement cache insertion
    }
}
