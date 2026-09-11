import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


def load_dm_settlement_report(**context):
    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    with database_engine.begin() as conn:
        conn.execute("TRUNCATE TABLE cdm.dm_settlement_report RESTART IDENTITY")

    sql = """
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
            r.id AS restaurant_id,
            r.restaurant_name,
            t.ts::date AS settlement_date,
            COUNT(DISTINCT o.id) AS orders_count,
            COALESCE(SUM(f.total_sum), 0) AS orders_total_sum,
            COALESCE(SUM(f.bonus_payment), 0) AS orders_bonus_payment_sum,
            COALESCE(SUM(f.bonus_grant), 0) AS orders_bonus_granted_sum,
            COALESCE(SUM(f.total_sum), 0) * 0.25 AS order_processing_fee,
            COALESCE(SUM(f.total_sum), 0)
              - COALESCE(SUM(f.bonus_payment), 0)
              - (COALESCE(SUM(f.total_sum), 0) * 0.25) AS restaurant_reward_sum
        FROM dds.dm_orders o
        INNER JOIN dds.fct_product_sales f
            ON o.id = f.order_id
        INNER JOIN dds.dm_restaurants r
            ON o.restaurant_id = r.id
        INNER JOIN dds.dm_timestamps t
            ON o.timestamp_id = t.id
        WHERE o.order_status = 'CLOSED'
        GROUP BY r.id, r.restaurant_name, t.ts::date
        ORDER BY r.id, t.ts::date
    """

    with database_engine.begin() as conn:
        conn.execute(sql)

    log.info("dm_settlement_report loaded successfully.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="cdm_settlement_report_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["cdm", "settlement"],
) as dag:

    task_load_dm_settlement_report = PythonOperator(
        task_id="load_dm_settlement_report",
        python_callable=load_dm_settlement_report,
    )
