alter table cdm.dm_settlement_report
    alter column orders_count set default 0,
    alter column orders_total_sum set default 0,
    alter column orders_bonus_payment_sum set default 0,
    alter column orders_bonus_granted_sum set default 0,
    alter column order_processing_fee set default 0,
    alter column restaurant_reward_sum set default 0,
    add constraint dm_settlement_report_orders_count_check check(orders_count >= 0),
    add constraint dm_settlement_report_orders_total_sum_check check(orders_total_sum >= (0)::numeric),
    add constraint dm_settlement_report_orders_bonus_payment_sum_check check(orders_bonus_payment_sum >= (0)::numeric),
    add constraint dm_settlement_report_orders_bonus_granted_sum_check check(orders_bonus_granted_sum >= (0)::numeric),
    add constraint dm_settlement_report_order_processing_fee_check check(order_processing_fee >= (0)::numeric),
    add constraint dm_settlement_report_restaurant_reward_sum_check check(restaurant_reward_sum >= (0)::numeric);