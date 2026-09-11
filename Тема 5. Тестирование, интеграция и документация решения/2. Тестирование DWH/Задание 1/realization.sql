SELECT
    CURRENT_TIMESTAMP AS test_date_time,
    'test_01' AS test_name,
    COUNT(*) = 0 AS test_result
FROM (
    SELECT *
    FROM public_test.dm_settlement_report_expected AS ex
    FULL JOIN public_test.dm_settlement_report_actual AS ac
        ON ex.restaurant_id = ac.restaurant_id
        AND ex.settlement_year = ac.settlement_year
        AND ex.settlement_month = ac.settlement_month
        AND ex.orders_count = ac.orders_count
        AND ex.orders_total_sum = ac.orders_total_sum
        AND ex.orders_bonus_payment_sum = ac.orders_bonus_payment_sum
        AND ex.orders_bonus_granted_sum = ac.orders_bonus_granted_sum
        AND ex.order_processing_fee = ac.order_processing_fee
        AND ex.restaurant_reward_sum = ac.restaurant_reward_sum
    WHERE ex.id IS NULL OR ac.id IS NULL
) AS T;