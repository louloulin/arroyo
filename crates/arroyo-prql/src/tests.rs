#[cfg(test)]
mod tests {
    use crate::is_prql_query;
    use crate::prql_to_sql;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_is_prql_query() {
        // PRQL queries
        assert!(is_prql_query("from employees"));
        assert!(is_prql_query("let x = 1\nfrom employees"));
        assert!(is_prql_query("from employees | filter age > 30"));
        assert!(is_prql_query("from employees\nfilter age > 30"));

        // SQL queries
        assert!(!is_prql_query("SELECT * FROM employees"));
        assert!(!is_prql_query("CREATE TABLE employees (id INT)"));
        assert!(!is_prql_query("INSERT INTO employees VALUES (1, 'John')"));
    }

    #[test]
    fn test_simple_prql_to_sql() {
        let prql = "from employees | filter age > 30 | select {name, age}";
        let expected_sql = "SELECT name, age FROM employees WHERE age > 30";

        let sql = prql_to_sql(prql).unwrap();
        // Remove whitespace for comparison
        let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let expected_sql = expected_sql.split_whitespace().collect::<Vec<_>>().join(" ");

        assert_eq!(sql, expected_sql);
    }

    #[test]
    fn test_prql_with_aggregation() {
        let prql = "from employees | group department (
            aggregate {
                avg_salary = average salary,
                count = count,
                total_salary = sum salary
            }
        ) | sort -total_salary";

        let sql = prql_to_sql(prql).unwrap();

        // Check that the SQL contains the expected elements
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("AVG"));
        assert!(sql.contains("COUNT"));
        assert!(sql.contains("SUM"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("ORDER BY"));
    }

    #[test]
    fn test_prql_with_window_functions() {
        let prql = "from employees |
            derive {
                department_avg = average salary over (partition department),
                rank = rank over (partition department order_by -salary)
            }";

        let sql = prql_to_sql(prql).unwrap();

        // Check that the SQL contains the expected elements
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("AVG"));
        assert!(sql.contains("OVER"));
        assert!(sql.contains("PARTITION BY"));
        assert!(sql.contains("RANK"));
    }

    #[test]
    fn test_prql_with_joins() {
        let prql = "from employees |
            join departments (==department_id) |
            select {employees.name, departments.name}";

        let sql = prql_to_sql(prql).unwrap();

        // Check that the SQL contains the expected elements
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("JOIN"));
        assert!(sql.contains("ON"));
    }

    #[test]
    fn test_prql_with_variables() {
        let prql = "
            let min_age = 30
            let max_salary = 100000

            from employees |
            filter age > min_age && salary < max_salary |
            select {name, age, salary}
        ";

        let sql = prql_to_sql(prql).unwrap();

        // Check that the SQL contains the expected elements
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("30"));
        assert!(sql.contains("100000"));
    }

    #[test]
    fn test_invalid_prql() {
        let prql = "from employees | invalid_operator";

        let result = prql_to_sql(prql);
        assert!(result.is_err());
    }
}
