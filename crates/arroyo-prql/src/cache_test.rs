#[cfg(test)]
mod tests {
    use crate::{add_to_cache, get_from_cache, prql_to_sql};

    #[test]
    fn test_cache_functionality() {
        // Clear cache before test
        if let Ok(mut cache) = crate::PRQL_CACHE.lock() {
            cache.clear();
        }

        // First query - should not be in cache
        let query = "from events\nselect {user_id, event_time}";
        let result1 = prql_to_sql(query).unwrap();
        
        // Check if query is now in cache
        let cached = get_from_cache(query);
        assert!(cached.is_some(), "Query should be in cache after first conversion");
        assert_eq!(cached.unwrap(), result1, "Cached result should match first conversion");
        
        // Second query with same input - should use cache
        let result2 = prql_to_sql(query).unwrap();
        assert_eq!(result1, result2, "Results should be identical");
        
        // Add custom entry to cache
        let custom_query = "from test\nselect {id}";
        let custom_sql = "SELECT id FROM test";
        add_to_cache(custom_query, custom_sql);
        
        // Verify custom entry
        let cached_custom = get_from_cache(custom_query);
        assert!(cached_custom.is_some(), "Custom query should be in cache");
        assert_eq!(cached_custom.unwrap(), custom_sql, "Cached result should match custom SQL");
    }
}
