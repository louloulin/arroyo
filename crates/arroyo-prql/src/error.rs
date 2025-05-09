//! Error types for PRQL conversion

use thiserror::Error;

/// Errors that can occur during PRQL to SQL conversion
#[derive(Error, Debug)]
pub enum PrqlError {
    /// Error during PRQL compilation
    #[error("PRQL compilation error: {0}")]
    CompilationError(String),

    /// Error during SQL post-processing
    #[error("SQL post-processing error: {0}")]
    PostProcessingError(String),

    /// Error related to Arroyo-specific features
    #[error("Arroyo feature error: {0}")]
    ArroyoFeatureError(String),

    /// Other errors
    #[error("Other error: {0}")]
    Other(String),
}

impl From<anyhow::Error> for PrqlError {
    fn from(err: anyhow::Error) -> Self {
        PrqlError::Other(err.to_string())
    }
}
