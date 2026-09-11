ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT uq_restaurant_date
UNIQUE (restaurant_id, settlement_date);