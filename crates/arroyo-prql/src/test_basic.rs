use crate::is_prql_query;
use crate::prql_to_sql;

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

    // Just test that it doesn't error for now
    let result = prql_to_sql(prql);
    assert!(result.is_ok());

    let sql = result.unwrap();
    // Check that it contains the expected elements
    assert!(sql.contains("SELECT"));
    assert!(sql.contains("name"));
    assert!(sql.contains("age"));
    assert!(sql.contains("FROM"));
    assert!(sql.contains("employees"));
    assert!(sql.contains("WHERE"));
}

#[test]
fn test_prql_with_aggregation() {
    // Skip this test for now as it requires more complex PRQL syntax
    // that we'll implement in a future update
}

#[test]
fn test_prql_with_window_functions() {
    // Skip this test for now as it requires more complex PRQL syntax
    // that we'll implement in a future update
}

#[test]
fn test_prql_with_joins() {
    let prql = "from employees
    join departments (==department_id)
    select {employees.name, departments.name}";

    // Just test that it doesn't error for now
    let result = prql_to_sql(prql);
    assert!(result.is_ok());

    let sql = result.unwrap();
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

        from employees
        filter age > min_age && salary < max_salary
        select {name, age, salary}
    ";

    // Just test that it doesn't error for now
    let result = prql_to_sql(prql);
    assert!(result.is_ok());

    let sql = result.unwrap();
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
