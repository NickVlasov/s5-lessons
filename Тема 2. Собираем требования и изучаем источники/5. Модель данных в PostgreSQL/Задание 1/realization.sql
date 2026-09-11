SELECT DISTINCT((event_value::jsonb)->'product_payments'->0->>'product_name') AS product_name
FROM outbox
WHERE event_value::JSON->>'product_payments' IS NOT NULL
