"""
DAG: load_courier_ledger
Расчёт витрины выплат курьерам (cdm.dm_courier_ledger) из DDS-слоя.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)


def load_cdm_courier_ledger(**context):
    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    conn = database_hook.get_conn()
    cur = conn.cursor()

    sql = """
        WITH order_data AS (
            SELECT
                c.courier_original_id AS courier_id,
                c.courier_name,
                EXTRACT(YEAR  FROM f.order_ts)::int AS settlement_year,
                EXTRACT(MONTH FROM f.order_ts)::int AS settlement_month,
                f."sum"   AS order_sum,
                f.tip_sum AS tip_sum,
                f.rate    AS rate
            FROM dds.fct_deliveries f
            JOIN dds.dm_couriers c ON f.courier_id = c.id
        ),
        monthly_agg AS (
            SELECT
                courier_id,
                courier_name,
                settlement_year,
                settlement_month,
                COUNT(*)              AS orders_count,
                SUM(order_sum)        AS orders_total_sum,
                ROUND(AVG(rate::numeric), 2)   AS rate_avg,
                SUM(tip_sum)          AS courier_tips_sum
            FROM order_data
            GROUP BY courier_id, courier_name, settlement_year, settlement_month
        ),
        order_payment AS (
            SELECT
                od.courier_id,
                od.settlement_year,
                od.settlement_month,
                CASE
                    WHEN ma.rate_avg < 4    THEN GREATEST(od.order_sum * 0.05, 100)
                    WHEN ma.rate_avg < 4.5  THEN GREATEST(od.order_sum * 0.07, 150)
                    WHEN ma.rate_avg < 4.9  THEN GREATEST(od.order_sum * 0.08, 175)
                    ELSE                         GREATEST(od.order_sum * 0.10, 200)
                END AS courier_order_payment
            FROM order_data od
            JOIN monthly_agg ma
                ON od.courier_id     = ma.courier_id
               AND od.settlement_year  = ma.settlement_year
               AND od.settlement_month = ma.settlement_month
        ),
        courier_order_sum_agg AS (
            SELECT
                courier_id,
                settlement_year,
                settlement_month,
                SUM(courier_order_payment) AS courier_order_sum
            FROM order_payment
            GROUP BY courier_id, settlement_year, settlement_month
        )
        INSERT INTO cdm.dm_courier_ledger (
            courier_id, courier_name, settlement_year, settlement_month,
            orders_count, orders_total_sum, rate_avg,
            order_processing_fee, courier_order_sum,
            courier_tips_sum, courier_reward_sum
        )
        SELECT
            ma.courier_id,
            ma.courier_name,
            ma.settlement_year,
            ma.settlement_month,
            ma.orders_count,
            ma.orders_total_sum,
            ma.rate_avg,
            ROUND(ma.orders_total_sum * 0.25, 2)                         AS order_processing_fee,
            ROUND(co.courier_order_sum, 2)                              AS courier_order_sum,
            ma.courier_tips_sum,
            ROUND(co.courier_order_sum + ma.courier_tips_sum * 0.95, 2) AS courier_reward_sum
        FROM monthly_agg ma
        JOIN courier_order_sum_agg co
            ON ma.courier_id     = co.courier_id
           AND ma.settlement_year  = co.settlement_year
           AND ma.settlement_month = co.settlement_month
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
            courier_name        = EXCLUDED.courier_name,
            orders_count        = EXCLUDED.orders_count,
            orders_total_sum    = EXCLUDED.orders_total_sum,
            rate_avg            = EXCLUDED.rate_avg,
            order_processing_fee = EXCLUDED.order_processing_fee,
            courier_order_sum   = EXCLUDED.courier_order_sum,
            courier_tips_sum    = EXCLUDED.courier_tips_sum,
            courier_reward_sum  = EXCLUDED.courier_reward_sum
    """

    cur.execute(sql)
    conn.commit()
    log.info(f"Loaded courier ledger. Rows affected: {cur.rowcount}")
    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_courier_ledger",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["dwh", "cdm", "courier_ledger"],
) as dag:

    task_load_courier_ledger = PythonOperator(
        task_id="load_cdm_courier_ledger",
        python_callable=load_cdm_courier_ledger,
    )
