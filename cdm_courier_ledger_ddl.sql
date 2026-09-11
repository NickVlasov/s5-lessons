CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    courier_id            text      NOT NULL,
    courier_name          text      NOT NULL,
    settlement_year       int       NOT NULL,
    settlement_month      int       NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count          int       NOT NULL,
    orders_total_sum      numeric(19, 2) NOT NULL,
    rate_avg              numeric(3, 2)  NOT NULL,
    order_processing_fee  numeric(19, 2) NOT NULL,
    courier_order_sum     numeric(19, 2) NOT NULL,
    courier_tips_sum      numeric(19, 2) NOT NULL,
    courier_reward_sum    numeric(19, 2) NOT NULL,
    CONSTRAINT uq_courier_ledger UNIQUE (courier_id, settlement_year, settlement_month) 
);
 