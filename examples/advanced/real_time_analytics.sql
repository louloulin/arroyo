-- real_time_analytics.sql
-- 这个示例展示了如何使用流处理进行实时数据分析

-- 步骤 1: 创建网站事件数据临时表
CREATE TEMPORARY TABLE website_events (
    event_id VARCHAR,
    user_id INT,
    session_id VARCHAR,
    event_type VARCHAR,
    page_id VARCHAR,
    product_id INT,
    category_id INT,
    event_time TIMESTAMP(3),
    referrer VARCHAR,
    device_type VARCHAR,
    browser VARCHAR,
    country VARCHAR,
    city VARCHAR,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 步骤 2: 插入一些测试数据
INSERT INTO website_events
SELECT 
    CONCAT('evt-', counter) AS event_id,
    CAST(RAND() * 1000 AS INT) AS user_id,
    CONCAT('session-', CAST(RAND() * 500 AS INT)) AS session_id,
    ARRAY['page_view', 'click', 'add_to_cart', 'remove_from_cart', 'purchase', 'search', 'login', 'logout', 'signup'][FLOOR(RAND() * 9) + 1] AS event_type,
    CONCAT('page-', CAST(RAND() * 50 AS INT)) AS page_id,
    CASE WHEN event_type IN ('click', 'add_to_cart', 'remove_from_cart', 'purchase') THEN CAST(RAND() * 200 AS INT) ELSE NULL END AS product_id,
    CAST(RAND() * 10 AS INT) AS category_id,
    CURRENT_TIMESTAMP - INTERVAL CAST(RAND() * 3600 AS INT) SECOND AS event_time,
    ARRAY['google.com', 'facebook.com', 'twitter.com', 'instagram.com', 'direct', 'email', 'affiliate'][FLOOR(RAND() * 7) + 1] AS referrer,
    ARRAY['desktop', 'mobile', 'tablet'][FLOOR(RAND() * 3) + 1] AS device_type,
    ARRAY['chrome', 'firefox', 'safari', 'edge', 'opera'][FLOOR(RAND() * 5) + 1] AS browser,
    ARRAY['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'CN'][FLOOR(RAND() * 10) + 1] AS country,
    ARRAY['New York', 'London', 'Toronto', 'Berlin', 'Paris', 'Tokyo', 'Sydney', 'Sao Paulo', 'Mumbai', 'Beijing'][FLOOR(RAND() * 10) + 1] AS city
FROM impulse()
LIMIT 10000;

-- 步骤 3: 实时分析 1 - 每分钟事件计数
-- 这个查询计算每分钟的事件数量，按事件类型分组
SELECT 
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    event_type,
    COUNT(*) AS event_count
FROM TABLE(
    TUMBLE(TABLE website_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY TUMBLE_START(event_time, INTERVAL '1' MINUTE), TUMBLE_END(event_time, INTERVAL '1' MINUTE), event_type
ORDER BY window_start, event_type;

-- 步骤 4: 实时分析 2 - 转化漏斗
-- 这个查询分析用户从页面浏览到购买的转化漏斗
WITH funnel_stages AS (
    SELECT 
        session_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicked,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
    FROM website_events
    GROUP BY session_id
)
SELECT 
    COUNT(*) AS total_sessions,
    SUM(viewed) AS view_count,
    SUM(clicked) AS click_count,
    SUM(added_to_cart) AS add_to_cart_count,
    SUM(purchased) AS purchase_count,
    SUM(clicked) * 100.0 / SUM(viewed) AS view_to_click_rate,
    SUM(added_to_cart) * 100.0 / SUM(clicked) AS click_to_cart_rate,
    SUM(purchased) * 100.0 / SUM(added_to_cart) AS cart_to_purchase_rate,
    SUM(purchased) * 100.0 / SUM(viewed) AS overall_conversion_rate
FROM funnel_stages;

-- 步骤 5: 实时分析 3 - 热门产品
-- 这个查询找出最受欢迎的产品，基于添加到购物车和购买事件
SELECT 
    product_id,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS view_count,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 0) AS view_to_cart_rate,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0) AS cart_to_purchase_rate
FROM website_events
WHERE product_id IS NOT NULL
GROUP BY product_id
HAVING add_to_cart_count > 0 OR purchase_count > 0
ORDER BY purchase_count DESC, add_to_cart_count DESC
LIMIT 10;

-- 步骤 6: 实时分析 4 - 地理分布
-- 这个查询分析用户的地理分布
SELECT 
    country,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(*) AS events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) * 100.0 / 
        COUNT(DISTINCT session_id) AS conversion_rate
FROM website_events
GROUP BY country
ORDER BY unique_users DESC;

-- 步骤 7: 实时分析 5 - 设备和浏览器分析
-- 这个查询分析不同设备和浏览器的使用情况
SELECT 
    device_type,
    browser,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(*) AS events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) * 100.0 / 
        COUNT(DISTINCT session_id) AS conversion_rate
FROM website_events
GROUP BY device_type, browser
ORDER BY sessions DESC;

-- 步骤 8: 实时分析 6 - 会话分析
-- 这个查询分析用户会话的持续时间和页面浏览数
WITH session_metrics AS (
    SELECT 
        session_id,
        MIN(event_time) AS session_start,
        MAX(event_time) AS session_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT page_id) AS unique_pages,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM website_events
    GROUP BY session_id
)
SELECT 
    TIMESTAMPDIFF(SECOND, session_start, session_end) / 60.0 AS session_duration_minutes,
    event_count,
    unique_pages,
    has_purchase,
    COUNT(*) AS session_count
FROM session_metrics
GROUP BY 
    FLOOR(TIMESTAMPDIFF(SECOND, session_start, session_end) / 60.0),
    event_count,
    unique_pages,
    has_purchase
ORDER BY session_count DESC
LIMIT 20;

-- 说明：
-- 1. 创建网站事件数据临时表，包含各种网站活动数据
-- 2. 插入测试数据，模拟网站用户行为
-- 3. 实时分析 1：计算每分钟的事件数量，按事件类型分组
-- 4. 实时分析 2：分析用户从页面浏览到购买的转化漏斗
-- 5. 实时分析 3：找出最受欢迎的产品，基于添加到购物车和购买事件
-- 6. 实时分析 4：分析用户的地理分布
-- 7. 实时分析 5：分析不同设备和浏览器的使用情况
-- 8. 实时分析 6：分析用户会话的持续时间和页面浏览数
-- 9. 这个示例展示了如何使用流处理进行实时数据分析，可以扩展为更复杂的分析系统
