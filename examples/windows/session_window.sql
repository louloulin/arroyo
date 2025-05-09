-- session_window.sql
-- 这个示例展示了如何使用会话窗口（由活动会话定义的窗口）进行聚合操作

-- 创建一个带有事件时间和水印的临时表
CREATE TEMPORARY TABLE user_activity (
    user_id INT,
    event_time TIMESTAMP(3),
    page_id VARCHAR,
    action VARCHAR,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);

-- 插入一些测试数据
-- 注意：为了更好地展示会话窗口，我们生成的数据会有明显的活动间隔
INSERT INTO user_activity
SELECT 
    CAST(RAND() * 20 AS INT) AS user_id,  -- 20个用户（ID 0-19）
    -- 生成有间隔的时间戳，模拟用户会话
    CURRENT_TIMESTAMP - INTERVAL (
        -- 基础时间：过去12小时内
        CAST(RAND() * 43200 AS INT) + 
        -- 添加会话分组：每个用户ID的活动集中在几个时间段
        (CAST(RAND() * 5 AS INT) * 3600 * (user_id % 5))
    ) SECOND AS event_time,
    CONCAT('page_', CAST(RAND() * 50 AS INT)) AS page_id,  -- 50个不同的页面
    ARRAY['view', 'click', 'scroll', 'type', 'submit'][FLOOR(RAND() * 5) + 1] AS action  -- 5种不同的动作
FROM impulse()
LIMIT 2000;

-- 使用会话窗口分析用户会话
-- 会话间隔为15分钟，即如果用户活动之间的间隔超过15分钟，则认为是新的会话
SELECT 
    user_id,
    SESSION_START(event_time, INTERVAL '15' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '15' MINUTE) AS session_end,
    COUNT(*) AS activity_count,
    COUNT(DISTINCT page_id) AS unique_pages,
    ARRAY_AGG(DISTINCT action) AS actions,
    ARRAY_AGG(DISTINCT page_id) AS pages_visited
FROM TABLE(
    SESSION(TABLE user_activity, DESCRIPTOR(event_time), INTERVAL '15' MINUTE)
)
GROUP BY user_id, SESSION_START(event_time, INTERVAL '15' MINUTE), SESSION_END(event_time, INTERVAL '15' MINUTE)
ORDER BY session_start, user_id;

-- 说明：
-- 1. 创建一个临时表 user_activity，包含用户活动数据（用户ID、时间、页面ID、动作）
-- 2. 为 event_time 字段定义水印，允许最多 5 秒的延迟
-- 3. 插入 2000 条随机测试数据，模拟 20 个用户在过去 12 小时内的活动，数据生成方式使活动有明显的时间间隔
-- 4. 使用 SESSION 函数创建会话窗口，会话间隔为 15 分钟
-- 5. 对每个用户的每个会话计算活动次数、访问的唯一页面数、执行的动作和访问的页面
-- 6. 会话窗口特点：动态大小，由活动间隔定义（这里是15分钟）
-- 7. 结果按会话开始时间和用户ID排序
-- 8. 会话窗口非常适合分析用户行为，因为它能自然地捕捉用户的活动模式
