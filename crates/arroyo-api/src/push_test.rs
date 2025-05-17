#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use cornucopia_async::DatabaseSource;
    use tower::ServiceExt;

    use crate::rest::create_rest_app;

    #[tokio::test]
    async fn test_push_api_integration() {
        // Create a mock database source
        let database = DatabaseSource::from_pool(None);

        // Create the app
        let app = create_rest_app(database, "http://localhost:8000");

        // Test the health endpoint
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/push/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Test the get topics endpoint
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/push/topics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Test creating a topic
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/push/topics")
                    .method("POST")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        r#"{"name":"test-topic","retention_period":604800,"compression":false}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        // Test getting a topic
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/push/topics/test-topic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Test pushing a message
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/push/test-topic")
                    .method("POST")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"message":"Hello, world!"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Test deleting a topic
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/push/topics/test-topic")
                    .method("DELETE")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
