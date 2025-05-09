# Arroyo PRQL 支持

Arroyo 现在支持 PRQL（Pipeline Relational Query Language），这是一种现代化的数据转换语言，旨在替代 SQL，提供更简洁、更强大的管道式查询能力。

## 什么是 PRQL？

PRQL 是一种流水线关系查询语言，具有以下特点：

- **管道式语法**：每一行代表对前一行结果的转换，形成逻辑流水线
- **简洁明了**：语法简洁，易于阅读和编写
- **强大的抽象**：支持变量、函数等抽象机制
- **类型安全**：提供类型检查和推断
- **数据库无关**：可编译为多种 SQL 方言

## PRQL 与 SQL 的对比

以下是一个简单的对比示例：

**SQL 查询**：
```sql
SELECT
  department,
  AVG(salary) AS avg_salary,
  COUNT(*) AS employee_count
FROM employees
WHERE age > 30
GROUP BY department
HAVING COUNT(*) > 5
ORDER BY avg_salary DESC
LIMIT 10
```

**等效的 PRQL 查询**：
```prql
from employees
filter age > 30
group department (
  aggregate {
    avg_salary = average salary,
    employee_count = count
  }
)
filter employee_count > 5
sort -avg_salary
take 10
```

## 在 Arroyo 中使用 PRQL

### 自动检测

Arroyo 可以自动检测查询是 SQL 还是 PRQL。当您提交查询时，系统会分析语法特征，如果看起来像 PRQL，则会自动将其转换为 SQL 执行。

### 显式指定查询类型

您也可以在创建管道时显式指定查询类型：

```json
{
  "name": "My PRQL Pipeline",
  "query": "from employees | filter age > 30 | select {name, age}",
  "query_type": "prql",
  "parallelism": 1
}
```

### Web UI 支持

在 Arroyo Web UI 中，您可以选择查询类型（SQL 或 PRQL），并享受语法高亮和错误提示等功能。

## PRQL 语法指南

### 基本语法

PRQL 查询由一系列转换组成，每个转换都对前一个转换的结果进行操作。转换之间使用管道符号 `|` 或换行符分隔。

```prql
from employees       # 数据源
filter age > 30      # 过滤条件
select {name, age}   # 选择列
```

### 常用转换

- **from**：指定数据源
- **filter**：过滤行
- **select**：选择或重命名列
- **derive**：添加新列
- **aggregate**：聚合计算
- **sort**：排序
- **take**：限制结果数量
- **join**：连接表
- **group**：分组

### 变量和函数

PRQL 支持变量和函数定义：

```prql
let min_age = 30
let max_salary = 100000

let is_eligible = age -> age > min_age

from employees
filter is_eligible age && salary < max_salary
select {name, age, salary}
```

## 流处理特定功能

Arroyo 的 PRQL 支持包括流处理特定的功能：

### 窗口操作

```prql
from events
window tumbling 5m (
  group user_id (
    aggregate {
      event_count = count
    }
  )
)
```

### 水印

```prql
from events
set_watermark event_time 5s
window tumbling 1m (
  group user_id (
    aggregate {
      event_count = count
    }
  )
)
```

### 连接器配置

```prql
from kafka "input-topic" (
  format json,
  bootstrap_servers "localhost:9092"
)
filter value > 10
into kafka "output-topic" (
  format json,
  bootstrap_servers "localhost:9092"
)
```

## 示例

### 基本过滤和投影

```prql
from employees
filter department == "Engineering" && salary > 100000
select {name, title, salary}
```

### 聚合

```prql
from orders
group {customer_id, product_id} (
  aggregate {
    order_count = count,
    total_amount = sum amount,
    avg_amount = average amount
  }
)
sort -total_amount
```

### 窗口聚合

```prql
from events
window tumbling 1h (
  group user_id (
    aggregate {
      event_count = count,
      distinct_pages = count_distinct page_id
    }
  )
)
```

### 连接

```prql
from orders
join order_items (==order_id)
join products (==product_id)
select {
  orders.order_id,
  orders.customer_id,
  products.name,
  order_items.quantity,
  order_items.price
}
```

### 派生列

```prql
from orders
derive {
  tax = amount * 0.1,
  total = amount + tax,
  order_date = date_trunc "day" timestamp
}
```

## 限制和注意事项

- 某些高级 SQL 功能可能尚未在 PRQL 中支持
- 复杂的窗口函数可能需要使用 SQL s-strings
- 自定义 UDF 需要在 SQL 中定义，然后在 PRQL 中使用

## 进一步学习

- [PRQL 官方文档](https://prql-lang.org/)
- [PRQL 语法参考](https://prql-lang.org/book/reference/syntax/)
- [PRQL 到 SQL 的转换](https://prql-lang.org/book/reference/target-sql.html)
