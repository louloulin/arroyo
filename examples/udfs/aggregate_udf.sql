-- aggregate_udf.sql
-- 这个示例展示了如何创建和使用聚合 UDF（用户定义函数）

-- 步骤 1: 创建 Rust 聚合 UDF - 中位数计算函数
CREATE AGGREGATE FUNCTION median(value DOUBLE) RETURNS DOUBLE
LANGUAGE RUST
AS $$
#[arroyo_udf]
struct MedianState {
    values: Vec<f64>,
}

#[arroyo_udf]
fn median_create() -> MedianState {
    MedianState { values: Vec::new() }
}

#[arroyo_udf]
fn median_accumulate(state: &mut MedianState, value: f64) {
    state.values.push(value);
}

#[arroyo_udf]
fn median_merge(state1: &mut MedianState, state2: &MedianState) {
    state1.values.extend_from_slice(&state2.values);
}

#[arroyo_udf]
fn median_emit(state: &MedianState) -> f64 {
    if state.values.is_empty() {
        return 0.0;
    }
    
    let mut values = state.values.clone();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    let n = values.len();
    if n % 2 == 0 {
        // 偶数个元素，取中间两个的平均值
        (values[n/2 - 1] + values[n/2]) / 2.0
    } else {
        // 奇数个元素，取中间值
        values[n/2]
    }
}
$$;

-- 步骤 2: 创建 Rust 聚合 UDF - 模式（众数）计算函数
CREATE AGGREGATE FUNCTION mode(value INT) RETURNS INT
LANGUAGE RUST
AS $$
use std::collections::HashMap;

#[arroyo_udf]
struct ModeState {
    counts: HashMap<i32, i32>,
}

#[arroyo_udf]
fn mode_create() -> ModeState {
    ModeState { counts: HashMap::new() }
}

#[arroyo_udf]
fn mode_accumulate(state: &mut ModeState, value: i32) {
    *state.counts.entry(value).or_insert(0) += 1;
}

#[arroyo_udf]
fn mode_merge(state1: &mut ModeState, state2: &ModeState) {
    for (value, count) in &state2.counts {
        *state1.counts.entry(*value).or_insert(0) += count;
    }
}

#[arroyo_udf]
fn mode_emit(state: &ModeState) -> i32 {
    if state.counts.is_empty() {
        return 0;
    }
    
    let mut max_count = 0;
    let mut mode_value = 0;
    
    for (value, count) in &state.counts {
        if *count > max_count {
            max_count = *count;
            mode_value = *value;
        }
    }
    
    mode_value
}
$$;

-- 步骤 3: 创建 Python 聚合 UDF - 字符串连接函数（带分隔符和最大长度）
CREATE AGGREGATE FUNCTION string_concat(value VARCHAR, delimiter VARCHAR, max_length INT) RETURNS VARCHAR
LANGUAGE PYTHON
AS $$
class StringConcatState:
    def __init__(self):
        self.result = ""
        self.delimiter = ""
        self.max_length = 0
        self.first = True

def string_concat_create():
    return StringConcatState()

def string_concat_accumulate(state, value, delimiter, max_length):
    if state.first:
        state.delimiter = delimiter
        state.max_length = max_length
        state.result = value
        state.first = False
    else:
        if len(state.result) + len(delimiter) + len(value) <= state.max_length:
            state.result += delimiter + value
        elif state.max_length > 0:
            # 如果超过最大长度，截断并添加省略号
            available_space = state.max_length - len(state.result) - len(delimiter) - 3
            if available_space > 0:
                state.result += delimiter + value[:available_space] + "..."
            else:
                # 如果没有足够空间，只添加省略号
                if not state.result.endswith("..."):
                    state.result = state.result[:state.max_length - 3] + "..."

def string_concat_merge(state1, state2):
    if state1.first:
        state1.result = state2.result
        state1.delimiter = state2.delimiter
        state1.max_length = state2.max_length
        state1.first = state2.first
    elif not state2.first:
        if state1.max_length > 0:
            if len(state1.result) + len(state1.delimiter) + len(state2.result) <= state1.max_length:
                state1.result += state1.delimiter + state2.result
            else:
                # 如果超过最大长度，截断并添加省略号
                available_space = state1.max_length - len(state1.result) - len(state1.delimiter) - 3
                if available_space > 0:
                    state1.result += state1.delimiter + state2.result[:available_space] + "..."
                else:
                    # 如果没有足够空间，只添加省略号
                    if not state1.result.endswith("..."):
                        state1.result = state1.result[:state1.max_length - 3] + "..."
        else:
            state1.result += state1.delimiter + state2.result

def string_concat_emit(state):
    return state.result
$$;

-- 步骤 4: 使用聚合 UDF 处理数据
-- 创建一个临时表用于测试
CREATE TEMPORARY TABLE test_data (
    id INT,
    category VARCHAR,
    value DOUBLE,
    score INT,
    tag VARCHAR
);

-- 插入一些测试数据
INSERT INTO test_data
SELECT 
    counter AS id,
    ARRAY['A', 'B', 'C', 'D', 'E'][FLOOR(RAND() * 5) + 1] AS category,
    RAND() * 100 AS value,
    CAST(RAND() * 10 AS INT) AS score,
    ARRAY['red', 'green', 'blue', 'yellow', 'black', 'white', 'orange', 'purple', 'pink', 'brown'][FLOOR(RAND() * 10) + 1] AS tag
FROM impulse()
LIMIT 100;

-- 使用聚合 UDF 查询数据
SELECT 
    category,
    COUNT(*) AS count,
    AVG(value) AS avg_value,
    median(value) AS median_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    mode(score) AS most_common_score,
    string_concat(tag, ', ', 50) AS tags
FROM test_data
GROUP BY category
ORDER BY category;

-- 使用窗口函数和聚合 UDF
SELECT 
    id,
    category,
    value,
    score,
    tag,
    median(value) OVER (PARTITION BY category) AS category_median,
    median(value) OVER (ORDER BY id ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS moving_median,
    mode(score) OVER (PARTITION BY category) AS category_mode
FROM test_data
ORDER BY category, id
LIMIT 20;

-- 说明：
-- 1. 创建了三个聚合用户定义函数 (UDF)：
--    - median：计算数值的中位数（Rust）
--    - mode：计算整数的众数（Rust）
--    - string_concat：连接字符串，带分隔符和最大长度限制（Python）
-- 2. 创建临时表并插入测试数据
-- 3. 使用聚合 UDF 处理数据，展示了如何在 GROUP BY 查询中使用聚合 UDF
-- 4. 展示了如何在窗口函数中使用聚合 UDF
-- 5. 聚合 UDF 的特点是处理多个输入行，返回单个聚合值
-- 6. 聚合 UDF 需要实现四个函数：create（创建状态）、accumulate（累积值）、merge（合并状态）和 emit（输出结果）
