"""
DAG: stg_delivery_api_load
Загрузка сырых данных из API курьерской службы в STG-слой.
Курьеры — полный справочник, доставки — за 7 предыдущих дней.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json 

# -----------------------------------------------------------------------------
# Конфигурация
# -----------------------------------------------------------------------------
API_BASE = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
NICKNAME = "anaklemis"
COHORT = "17"
LIMIT = 50
DAYS_BACK = 7

# !!! ВСТАВЬ СЮДА СВОЙ API-КЛЮЧ (только для теста в учебном проекте) !!!
API_KEY = "25c27781-8fde-4b30-a22e-524044a7580f"

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# -----------------------------------------------------------------------------
# Универсальная функция пагинации
# -----------------------------------------------------------------------------
def fetch_all_pages(endpoint, sort_field, ti):
    """Обходит все страницы API и возвращает список всех записей."""
    headers = {
        "X-Nickname": NICKNAME,
        "X-Cohort": COHORT,
        "X-API-KEY": API_KEY,  # <-- теперь берём из переменной в коде
    }

    offset = 0
    all_records = []

    while True:
        params = {
            "sort_field": sort_field,
            "sort_direction": "asc",
            "limit": LIMIT,
            "offset": offset,
        }
        resp = requests.get(
            f"{API_BASE}/{endpoint}",
            headers=headers,
            params=params,
            timeout=30,
        )
        # Если ошибка — сразу выбрасываем, чтобы видеть в логах
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        all_records.extend(data)
        ti.xcom_push(key="page_info", value={
            "endpoint": endpoint,
            "offset": offset,
            "records_on_page": len(data),
        })

        if len(data) < LIMIT:
            break

        offset += LIMIT

    ti.xcom_push(key="total_records", value=len(all_records))
    return all_records

# -----------------------------------------------------------------------------
# Вставка в STG
# -----------------------------------------------------------------------------
def insert_to_stg(pg_hook, table, records, id_field):
    """Вставка сырых JSON в STG-таблицу."""
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    insert_sql = f"""
        INSERT INTO stg.{table} (object_id, object_value, load_ts)
        VALUES (%s, %s, now())
    """

    batch = []
    for r in records:
        batch.append((r[id_field], json.dumps(r)))

    if batch:
        cur.executemany(insert_sql, batch)

    conn.commit()
    cur.close()
    conn.close()

# -----------------------------------------------------------------------------
# Таски
# -----------------------------------------------------------------------------
def load_couriers(**context):
    ti = context["ti"]
    records = fetch_all_pages("couriers", "_id", ti)
    pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    insert_to_stg(pg_hook, "api_couriers", records, "_id")
    print(f"Загружено курьеров в STG: {len(records)}")

def load_deliveries(**context):
    ti = context["ti"]
    records = fetch_all_pages("deliveries", "delivery_id", ti)

    today = datetime.now().date()
    date_from = today - timedelta(days=DAYS_BACK)

    filtered = []
    for r in records:
        order_ts_str = r.get("order_ts")
        if not order_ts_str:
            continue
        order_date = datetime.strptime(order_ts_str[:10], "%Y-%m-%d").date()
        if date_from <= order_date <= today:
            filtered.append(r)

    pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    insert_to_stg(pg_hook, "api_deliveries", filtered, "delivery_id")
    print(f"Загружено доставок в STG: {len(filtered)} (из {len(records)} полученных)")

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="stg_delivery_api_load",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["stg", "delivery", "api"],
) as dag:

    t_load_couriers = PythonOperator(
        task_id="load_couriers",
        python_callable=load_couriers,
    )

    t_load_deliveries = PythonOperator(
        task_id="load_deliveries",
        python_callable=load_deliveries,
    )

    t_load_couriers >> t_load_deliveries
