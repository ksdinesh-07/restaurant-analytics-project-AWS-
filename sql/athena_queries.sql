-- ============================================
-- RESTAURANT ANALYTICS - ATHENA QUERIES
-- ============================================

-- 1. Basic Data Check
SELECT dt, name, city, gmv, late_rate
FROM food_analytics.daily_restaurant_metrics
ORDER BY dt DESC
LIMIT 10;

-- 2. GMV Trend by City
SELECT
    dt,
    city,
    SUM(gmv) AS total_gmv,
    SUM(orders_delivered) AS total_orders
FROM food_analytics.daily_restaurant_metrics
GROUP BY dt, city
ORDER BY dt, total_gmv DESC;

-- 3. Top 5 Restaurants by GMV (per day)
SELECT *
FROM (
    SELECT
        dt,
        name,
        city,
        gmv,
        RANK() OVER (PARTITION BY dt ORDER BY gmv DESC) AS rank
    FROM food_analytics.daily_restaurant_metrics
) t
WHERE rank <= 5
ORDER BY dt, rank;

-- 4. Late Delivery Analysis by Cuisine
SELECT
    cuisine,
    SUM(late_count) AS total_late_orders,
    SUM(orders_delivered) AS total_delivered,
    ROUND(SUM(late_count) * 100.0 / SUM(orders_delivered), 2) AS late_rate_pct
FROM food_analytics.daily_restaurant_metrics
GROUP BY cuisine
ORDER BY late_rate_pct DESC;

-- 5. City Performance Dashboard
SELECT
    city,
    COUNT(DISTINCT restaurant_id) AS active_restaurants,
    SUM(orders_delivered) AS total_orders,
    ROUND(SUM(gmv), 2) AS total_revenue,
    ROUND(AVG(avg_delivery_mins), 1) AS avg_delivery_time,
    ROUND(AVG(late_rate) * 100, 1) AS avg_late_rate_pct
FROM food_analytics.daily_restaurant_metrics
GROUP BY city
ORDER BY total_revenue DESC;

-- 6. Daily Performance Summary
SELECT
    dt,
    COUNT(DISTINCT restaurant_id) AS restaurants_active,
    SUM(orders_delivered) AS total_orders,
    ROUND(SUM(gmv), 2) AS total_revenue,
    ROUND(AVG(avg_delivery_mins), 1) AS avg_delivery_minutes,
    ROUND(AVG(late_rate) * 100, 1) AS avg_late_rate_pct
FROM food_analytics.daily_restaurant_metrics
GROUP BY dt
ORDER BY dt DESC;

-- 7. Create Table Schema
CREATE EXTERNAL TABLE IF NOT EXISTS food_analytics.daily_restaurant_metrics (
    restaurant_id int,
    name string,
    cuisine string,
    city string,
    orders_delivered int,
    gmv bigint,
    avg_delivery_mins double,
    late_count int,
    late_rate double
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION 's3://restaurant-gold/daily_restaurant_metrics/';

-- 8. Load Partitions
MSCK REPAIR TABLE food_analytics.daily_restaurant_metrics;
