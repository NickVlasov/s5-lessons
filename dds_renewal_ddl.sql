-- ==========================================
-- DDS RENEWAL: новые и изменённые таблицы
-- для витрины расчётов с курьерами
-- ==========================================

-- ==========================================
-- 1. НОВАЯ ТАБЛИЦА: dds.dm_couriers
--    Измерение «Курьер»
-- ==========================================
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id                  int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    courier_original_id text NOT NULL UNIQUE,
    courier_name        text NOT NULL,
    load_ts             timestamp NOT NULL DEFAULT now()
);

-- ==========================================
-- 2. НОВАЯ ТАБЛИЦА: dds.fct_deliveries
--    Факт «Доставка»
-- ==========================================
CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id                   int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_original_id text NOT NULL UNIQUE,
    order_id             int REFERENCES dds.dm_orders(id),
    courier_id           int REFERENCES dds.dm_couriers(id),
    order_ts             timestamp NOT NULL,          -- дата заказа (без зоны)
    delivery_ts          timestamp,                    -- дата доставки (без зоны)
    address              text,
    rate                 numeric(3, 2) NOT NULL,
    sum                  numeric(19, 2) NOT NULL,
    tip_sum              numeric(19, 2) NOT NULL,
    load_ts              timestamp NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fct_deliveries_courier
    ON dds.fct_deliveries (courier_id);

CREATE INDEX IF NOT EXISTS idx_fct_deliveries_order
    ON dds.fct_deliveries (order_id);

-- ==========================================
-- 3. ИЗМЕНЕНИЕ СУЩЕСТВУЮЩЕЙ: dds.dm_orders
--    Добавляем ссылку на курьера
-- ==========================================
ALTER TABLE dds.dm_orders
    ADD COLUMN IF NOT EXISTS courier_id int REFERENCES dds.dm_couriers(id);
