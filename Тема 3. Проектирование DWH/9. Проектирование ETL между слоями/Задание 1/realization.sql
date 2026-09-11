INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    r.restaurant_id,
    r.restaurant_name,
    t.date AS settlement_date,
    COUNT(DISTINCT s.order_id) AS orders_count,
    COALESCE(SUM(s.total_sum), 0) AS orders_total_sum,
    COALESCE(SUM(s.bonus_payment), 0) AS orders_bonus_payment_sum,
    COALESCE(SUM(s.bonus_grant), 0) AS orders_bonus_granted_sum,
    COALESCE(SUM(s.total_sum), 0) * 0.25 AS order_processing_fee,
    (COALESCE(SUM(s.total_sum), 0) * 0.75) - COALESCE(SUM(s.bonus_payment), 0) AS restaurant_reward_sum
FROM dds.fct_product_sales s
JOIN dds.dm_orders o ON s.order_id = o.id
JOIN dds.dm_products p ON s.product_id = p.id
JOIN dds.dm_restaurants r ON p.restaurant_id = r.id
JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
WHERE o.order_status = 'CLOSED'
GROUP BY
    r.restaurant_id,
    r.restaurant_name,
    t.date
ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
