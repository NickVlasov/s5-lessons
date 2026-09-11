-- ==========================================
-- STG RENEWAL: сырые данные из API
-- Храним JSON как есть, вытаскиваем ID
-- ==========================================

-- ==========================================
-- 1. stg.api_couriers
--    Сырые данные курьеров из GET /couriers
-- ==========================================
CREATE TABLE IF NOT EXISTS stg.api_couriers (
    id           serial PRIMARY KEY,
    object_id    text NOT NULL,            -- _id из API (бизнес-ключ)
    object_value jsonb NOT NULL,           -- весь объект целиком
    load_ts      timestamp NOT NULL DEFAULT now()
);

-- ==========================================
-- 2. stg.api_deliveries
--    Сырые данные доставок из GET /deliveries
-- ==========================================
CREATE TABLE IF NOT EXISTS stg.api_deliveries (
    id           serial PRIMARY KEY,
    object_id    text NOT NULL,            -- delivery_id из API (бизнес-ключ)
    object_value jsonb NOT NULL,           -- весь объект целиком
    load_ts      timestamp NOT NULL DEFAULT now() 
);
