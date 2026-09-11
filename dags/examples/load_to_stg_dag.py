from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime, timedelta
import logging
import json 

log = logging.getLogger(__name__)

# --- Универсальная функция (DRY: Don't Repeat Yourself) ---
def load_table(table_name, columns, **context):
    """
    Загружает указанную таблицу из источника в stg.
    :param table_name: Имя таблицы в источнике и целевое имя (без схемы)
    :param columns: Список колонок для выборки (строка SQL)
    """
    src_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    dst_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    src_engine = src_hook.get_sqlalchemy_engine()
    dst_engine = dst_hook.get_sqlalchemy_engine()

    # Формируем SQL-запрос динамически
    sql_query = f"SELECT {columns} FROM {table_name}"
    
    with src_engine.connect() as src_conn:
        log.info(f"Reading data from source table: {table_name}")
        df = pd.read_sql(sql_query, con=src_conn)
        log.info(f"Read {len(df)} rows from source.")

        if df.empty:
            log.warning("No rows to load; skipping insert.")
            return

    # Очищаем целевую таблицу (stg.<table_name>)
    truncate_query = f"TRUNCATE TABLE stg.bonussystem_{table_name};"
    dst_hook.run(truncate_query)
    log.info(f"Staging table truncated: stg.bonussystem_{table_name}")

    # Загружаем данные
    df.to_sql(
        name=f"bonussystem_{table_name}",
        con=dst_engine,
        schema="stg",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    log.info(f"Inserted {len(df)} rows into stg.bonussystem_{table_name}.")

def check_events(**context):
    src_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    dst_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    src_engine = src_hook.get_sqlalchemy_engine()
    dst_engine = dst_hook.get_sqlalchemy_engine()

    workflow_key = "bonus_system_events"

    # 1. Читаем текущий прогресс
    with dst_engine.connect() as dst_conn:
        result = pd.read_sql(
            f"SELECT workflow_settings FROM stg.srv_wf_settings WHERE workflow_key = '{workflow_key}' ORDER BY id DESC LIMIT 1;",
            con=dst_conn
        )

    if result.empty:
        actual_id = 0
    else:
        settings = json.loads(result["workflow_settings"].iloc[0])
        actual_id = settings.get("last_id", 0)

    log.info(f"Actual id from settings: {actual_id}")

    # 2. Читаем новые строки из источника
    with src_engine.connect() as src_conn:
        df = pd.read_sql(
            f"SELECT id, event_ts, event_type, event_value FROM outbox WHERE id > {actual_id};",
            con=src_conn
        )
        log.info(f"Read {len(df)} rows from source.")

    if df.empty:
        log.warning("No new rows to load.")
        return

    # 3. Записываем в stg
    df.to_sql(
        name="bonussystem_events",
        con=dst_engine,
        schema="stg",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    log.info(f"Inserted {len(df)} rows into stg.bonussystem_events.")

    # 4. UPSERT: вставляем или обновляем прогресс
    new_actual_id = int(df["id"].max())
    new_settings = json.dumps({"last_id": new_actual_id})

    dst_hook.run(f"""
        INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
        VALUES ('{workflow_key}', '{new_settings}');
    """)
    log.info(f"Workflow settings inserted: last_id = {new_actual_id}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- ОДИН DAG для обеих задач ---
with DAG(
    dag_id="load_all_to_stg",  # Новое имя DAG, так как он делает больше, чем просто ranks
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["dwh", "stg"],
) as dag:

    # Задача 1: Загрузка ranks
    task_load_ranks = PythonOperator(
        task_id="load_ranks",
        python_callable=load_table,
        op_kwargs={
            "table_name": "ranks",
            "columns": "id, name, bonus_percent, min_payment_threshold"
        },
    )

    # Задача 2: Загрузка users
    task_load_users = PythonOperator(
        task_id="load_users",
        python_callable=load_table,
        op_kwargs={
            "table_name": "users",
            "columns": "id, order_user_id"
        },
    )

    task_load_events = PythonOperator(
        task_id='events_load',
        python_callable=check_events,
    )

    # --- Правильная связь задач ---
    # Сначала грузим ranks, потом users
    task_load_ranks >> task_load_users >> task_load_events
