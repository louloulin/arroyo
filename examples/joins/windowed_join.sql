-- windowed_join.sql
-- 这个示例展示了如何在时间窗口内连接多个数据流，处理时间上相关的事件

-- 创建网站点击数据临时表
CREATE TEMPORARY TABLE website_clicks (
    user_id INT,
    event_time TIMESTAMP(3),
    page_id VARCHAR,
    session_id VARCHAR,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 创建广告展示数据临时表
CREATE TEMPORARY TABLE ad_impressions (
    ad_id INT,
    user_id INT,
    event_time TIMESTAMP(3),
    campaign_id VARCHAR,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 创建转化数据临时表
CREATE TEMPORARY TABLE conversions (
    conversion_id INT,
    user_id INT,
    event_time TIMESTAMP(3),
    conversion_type VARCHAR,
    value DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些网站点击测试数据
INSERT INTO website_clicks
SELECT 
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    CONCAT('page_', CAST(RAND() * 20 AS INT)) AS page_id,
    CONCAT('session_', CAST(RAND() * 200 AS INT)) AS session_id
FROM impulse()
LIMIT 1000;

-- 插入一些广告展示测试数据
INSERT INTO ad_impressions
SELECT 
    counter AS ad_id,
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    CONCAT('campaign_', CAST(RAND() * 10 AS INT)) AS campaign_id
FROM impulse()
LIMIT 500;

-- 插入一些转化测试数据
INSERT INTO conversions
SELECT 
    counter AS conversion_id,
    CAST(RAND() * 100 AS INT) AS user_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    ARRAY['purchase', 'signup', 'download', 'subscription'][FLOOR(RAND() * 4) + 1] AS conversion_type,
    10 + RAND() * 90 AS value
FROM impulse()
LIMIT 200;

-- 1. 使用滚动窗口连接点击和转化事件
-- 分析每个10分钟窗口内的点击到转化路径
SELECT 
    TUMBLE_START(c.event_time, INTERVAL '10' MINUTE) AS window_start,
    TUMBLE_END(c.event_time, INTERVAL '10' MINUTE) AS window_end,
    c.user_id,
    c.page_id,
    c.session_id,
    v.conversion_type,
    v.value,
    v.event_time - c.event_time AS time_to_conversion
FROM TABLE(
    TUMBLE(TABLE website_clicks, DESCRIPTOR(event_time), INTERVAL '10' MINUTE)
) AS c
JOIN TABLE(
    TUMBLE(TABLE conversions, DESCRIPTOR(event_time), INTERVAL '10' MINUTE)
) AS v
ON c.user_id = v.user_id AND c.window_start = v.window_start AND c.window_end = v.window_end
ORDER BY c.event_time
LIMIT 20;

-- 2. 使用滑动窗口分析广告展示和转化的关系
-- 使用30分钟窗口，每5分钟滑动一次
SELECT 
    HOP_START(a.event_time, INTERVAL '5' MINUTE, INTERVAL '30' MINUTE) AS window_start,
    HOP_END(a.event_time, INTERVAL '5' MINUTE, INTERVAL '30' MINUTE) AS window_end,
    a.campaign_id,
    COUNT(DISTINCT a.user_id) AS unique_users,
    COUNT(DISTINCT v.conversion_id) AS conversion_count,
    SUM(v.value) AS conversion_value,
    COUNT(DISTINCT v.conversion_id) * 100.0 / COUNT(DISTINCT a.user_id) AS conversion_rate
FROM TABLE(
    HOP(TABLE ad_impressions, DESCRIPTOR(event_time), INTERVAL '5' MINUTE, INTERVAL '30' MINUTE)
) AS a
LEFT JOIN TABLE(
    HOP(TABLE conversions, DESCRIPTOR(event_time), INTERVAL '5' MINUTE, INTERVAL '30' MINUTE)
) AS v
ON a.user_id = v.user_id AND a.window_start = v.window_start AND a.window_end = v.window_end
GROUP BY a.campaign_id, HOP_START(a.event_time, INTERVAL '5' MINUTE, INTERVAL '30' MINUTE), HOP_END(a.event_time, INTERVAL '5' MINUTE, INTERVAL '30' MINUTE)
ORDER BY window_start, conversion_rate DESC;

-- 3. 三流连接：在会话窗口内连接点击、广告展示和转化
-- 分析用户在一个会话中的完整行为路径
WITH user_sessions AS (
    SELECT 
        user_id,
        SESSION_START(event_time, INTERVAL '20' MINUTE) AS session_start,
        SESSION_END(event_time, INTERVAL '20' MINUTE) AS session_end
    FROM TABLE(
        SESSION(TABLE website_clicks, DESCRIPTOR(event_time), INTERVAL '20' MINUTE)
    )
    GROUP BY user_id, SESSION_START(event_time, INTERVAL '20' MINUTE), SESSION_END(event_time, INTERVAL '20' MINUTE)
)
SELECT 
    s.user_id,
    s.session_start,
    s.session_end,
    COUNT(DISTINCT c.page_id) AS pages_visited,
    STRING_AGG(DISTINCT c.page_id, ', ') AS pages,
    COUNT(DISTINCT a.ad_id) AS ads_seen,
    STRING_AGG(DISTINCT a.campaign_id, ', ') AS campaigns,
    COUNT(DISTINCT v.conversion_id) AS conversions,
    STRING_AGG(DISTINCT v.conversion_type, ', ') AS conversion_types,
    SUM(v.value) AS total_value
FROM user_sessions s
LEFT JOIN website_clicks c 
    ON s.user_id = c.user_id AND c.event_time BETWEEN s.session_start AND s.session_end
LEFT JOIN ad_impressions a 
    ON s.user_id = a.user_id AND a.event_time BETWEEN s.session_start AND s.session_end
LEFT JOIN conversions v 
    ON s.user_id = v.user_id AND v.event_time BETWEEN s.session_start AND s.session_end
GROUP BY s.user_id, s.session_start, s.session_end
HAVING COUNT(DISTINCT v.conversion_id) > 0  -- 只显示有转化的会话
ORDER BY total_value DESC
LIMIT 10;

-- 说明：
-- 1. 创建三个临时表：website_clicks（网站点击数据）、ad_impressions（广告展示数据）和 conversions（转化数据）
-- 2. 为三个表的 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入测试数据，模拟用户在网站上的行为
-- 4. 第一个查询：使用滚动窗口连接点击和转化事件，分析每个10分钟窗口内的点击到转化路径
-- 5. 第二个查询：使用滑动窗口分析广告展示和转化的关系，计算每个广告活动的转化率
-- 6. 第三个查询：三流连接，在会话窗口内连接点击、广告展示和转化，分析用户在一个会话中的完整行为路径
-- 7. 窗口连接特点：在时间窗口内连接多个数据流，处理时间上相关的事件
-- 8. 这种连接方式非常适合分析用户行为路径、营销效果和多渠道归因
