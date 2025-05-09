-- window_functions.sql
-- 这个示例展示了如何使用窗口函数（如 ROW_NUMBER、RANK、LEAD、LAG 等）进行分析操作

-- 创建一个带有事件时间和水印的临时表
CREATE TEMPORARY TABLE web_traffic (
    user_id INT,
    session_id VARCHAR,
    event_time TIMESTAMP(3),
    page_id VARCHAR,
    duration_sec INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些测试数据
INSERT INTO web_traffic
SELECT 
    CAST(RAND() * 50 AS INT) AS user_id,  -- 50个用户（ID 0-49）
    CONCAT('session_', CAST(RAND() * 100 AS INT)) AS session_id,  -- 100个会话
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 86400 AS INT) SECOND AS event_time,  -- 过去24小时内的随机时间
    CONCAT('page_', CAST(RAND() * 20 AS INT)) AS page_id,  -- 20个页面
    CAST(RAND() * 300 AS INT) AS duration_sec  -- 0-300秒的随机停留时间
FROM impulse()
LIMIT 1000;

-- 1. 使用 ROW_NUMBER 窗口函数为每个用户的页面访问分配序号
SELECT 
    user_id,
    event_time,
    page_id,
    duration_sec,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) AS visit_sequence,
    ROW_NUMBER() OVER (PARTITION BY user_id, page_id ORDER BY event_time) AS page_visit_sequence
FROM web_traffic
ORDER BY user_id, event_time
LIMIT 20;

-- 2. 使用 RANK 和 DENSE_RANK 窗口函数对页面停留时间进行排名
SELECT 
    page_id,
    AVG(duration_sec) AS avg_duration,
    RANK() OVER (ORDER BY AVG(duration_sec) DESC) AS duration_rank,
    DENSE_RANK() OVER (ORDER BY AVG(duration_sec) DESC) AS duration_dense_rank
FROM web_traffic
GROUP BY page_id
ORDER BY avg_duration DESC;

-- 3. 使用 LEAD 和 LAG 窗口函数分析用户的页面导航路径
SELECT 
    user_id,
    session_id,
    event_time,
    page_id,
    LAG(page_id) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS previous_page,
    LEAD(page_id) OVER (PARTITION BY user_id, session_id ORDER BY event_time) AS next_page,
    duration_sec,
    LEAD(event_time) OVER (PARTITION BY user_id, session_id ORDER BY event_time) - event_time AS time_to_next_page
FROM web_traffic
ORDER BY user_id, session_id, event_time
LIMIT 20;

-- 4. 使用窗口聚合函数计算累计和移动平均值
SELECT 
    event_time,
    EXTRACT(HOUR FROM event_time) AS hour_of_day,
    COUNT(*) AS visits_in_hour,
    SUM(COUNT(*)) OVER (ORDER BY EXTRACT(HOUR FROM event_time)) AS cumulative_visits,
    AVG(COUNT(*)) OVER (
        ORDER BY EXTRACT(HOUR FROM event_time)
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3hour
FROM web_traffic
GROUP BY EXTRACT(HOUR FROM event_time), event_time
ORDER BY hour_of_day;

-- 5. 使用窗口函数计算百分比和分位数
SELECT 
    page_id,
    COUNT(*) AS visit_count,
    SUM(duration_sec) AS total_duration,
    ROUND(
        100.0 * SUM(duration_sec) / SUM(SUM(duration_sec)) OVER (),
        2
    ) AS percentage_of_total_duration,
    NTILE(4) OVER (ORDER BY SUM(duration_sec) DESC) AS duration_quartile
FROM web_traffic
GROUP BY page_id
ORDER BY total_duration DESC;

-- 说明：
-- 1. 创建一个临时表 web_traffic，包含网站流量数据（用户ID、会话ID、时间、页面ID、停留时间）
-- 2. 为 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入 1000 条随机测试数据，模拟 50 个用户在过去 24 小时内的网站访问
-- 4. 第一个查询：使用 ROW_NUMBER 窗口函数为每个用户的页面访问分配序号
-- 5. 第二个查询：使用 RANK 和 DENSE_RANK 窗口函数对页面停留时间进行排名
-- 6. 第三个查询：使用 LEAD 和 LAG 窗口函数分析用户的页面导航路径
-- 7. 第四个查询：使用窗口聚合函数计算累计和移动平均值
-- 8. 第五个查询：使用窗口函数计算百分比和分位数
-- 9. 窗口函数特点：在查询结果集上进行计算，不改变结果集的行数，适合进行排名、导航分析等操作
