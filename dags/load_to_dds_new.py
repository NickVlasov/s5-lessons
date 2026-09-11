from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text  # --- НОВОЕ ---
import logging
import json

log = logging.getLogger(__name__)

def load_dm_users(stg_table_name: str, dds_table_name: str, **context):

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)
    log.info(f"Read {len(df)} rows from stg.{stg_table_name}")

    if df.empty:
        log.warning("No rows to load; skipping insert.")
        return

    records = []
    for _, row in df.iterrows():
        obj = json.loads(row["object_value"])
        records.append({
            "user_id": str(obj["_id"]),
            "user_name": obj.get("name"),
            "user_login": obj.get("login"),
        })

    df_new = pd.DataFrame(records)

    with database_engine.connect() as conn:
        existing = pd.read_sql_query(
            f"SELECT user_id FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new[~df_new["user_id"].isin(existing["user_id"])]

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new rows to insert.")

def load_dm_restaurants(stg_table_name: str, dds_table_name: str, **context):

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value, update_ts FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)
    log.info(f"Read {len(df)} rows from stg.{stg_table_name}")

    if df.empty:
        log.warning("No rows to load; skipping insert.")
        return

    records = []
    for _, row in df.iterrows():
        val = row["object_value"]
        obj = val if isinstance(val, dict) else json.loads(val)
        records.append({
            "restaurant_id": str(obj["_id"]),
            "restaurant_name": obj.get("name"),
            "active_from": row["update_ts"],
            "active_to": datetime(2099, 12, 31),
        })

    df_new = pd.DataFrame(records)

    with database_engine.connect() as conn:
        existing = pd.read_sql_query(
            f"SELECT restaurant_id FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new[~df_new["restaurant_id"].isin(existing["restaurant_id"])]

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new rows to insert.")

def load_dm_timestamps(stg_table_name: str, dds_table_name: str, **context):

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)
    log.info(f"Read {len(df)} rows from stg.{stg_table_name}")

    if df.empty:
        log.warning("No rows to load; skipping insert.")
        return

    records = []
    for _, row in df.iterrows():
        val = row["object_value"]
        obj = val if isinstance(val, dict) else json.loads(val)

        raw_date = obj.get("date")
        if not raw_date:
            continue

        ts = pd.to_datetime(raw_date)

        records.append({
            "ts": ts,
            "year": ts.year,
            "month": ts.month,
            "day": ts.day,
            "date": ts.date(),
            "time": ts.time(),
        })
    df_new = pd.DataFrame(records)

    if df_new.empty:
        log.warning("No valid timestamps found; skipping insert.")
        return

    with database_engine.connect() as conn:
        existing = pd.read_sql_query(
            f"SELECT ts FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new[~df_new["ts"].isin(existing["ts"])]

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new rows to insert.")

def load_dm_products(stg_table_name: str, dds_table_name: str, **context):

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)

    if df.empty:
        log.warning("No orders in STG; skipping.")
        return

    with database_engine.connect() as conn:
        df_rest = pd.read_sql("SELECT id, restaurant_id FROM dds.dm_restaurants", con=conn)
    rest_map = dict(zip(df_rest["restaurant_id"], df_rest["id"]))

    records = []
    for _, row in df.iterrows():
        val = row["object_value"]
        obj = val if isinstance(val, dict) else json.loads(val)

        rest_source_id = obj.get("restaurant", {}).get("id")
        if not rest_source_id or rest_source_id not in rest_map:
            continue

        surrogate_rest_id = rest_map[rest_source_id]
        update_ts = obj.get("update_ts")
        if not update_ts:
            continue

        active_from = pd.to_datetime(update_ts)

        order_items = obj.get("order_items", [])
        for item in order_items:
            product_id = item.get("id")
            if not product_id:
                continue

            records.append({
                "product_id": product_id,
                "product_name": item.get("name"),
                "product_price": item.get("price"),
                "active_from": active_from,
                "active_to": datetime(2099, 12, 31),
                "restaurant_id": surrogate_rest_id
            })

    df_new = pd.DataFrame(records)
    if df_new.empty:
        log.warning("No products to load; skipping.")
        return

    df_new = df_new.drop_duplicates(
        subset=["product_name", "restaurant_id"],
        keep="first"
    )

    with database_engine.connect() as conn:
        existing = pd.read_sql(
            f"SELECT product_name, restaurant_id FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new.merge(
        existing, on=["product_name", "restaurant_id"], how="left", indicator=True
    )
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new products to insert.")

def load_dm_orders(stg_table_name: str, dds_table_name: str, **context):

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)

    if df.empty:
        log.warning("No orders in STG; skipping.")
        return

    with database_engine.connect() as conn:
        df_rest = pd.read_sql("SELECT id, restaurant_id FROM dds.dm_restaurants", con=conn)
        df_users = pd.read_sql("SELECT id, user_id FROM dds.dm_users", con=conn)
        df_ts = pd.read_sql("SELECT id, ts FROM dds.dm_timestamps", con=conn)

    rest_map = dict(zip(df_rest["restaurant_id"], df_rest["id"]))
    user_map = dict(zip(df_users["user_id"], df_users["id"]))
    ts_map = dict(zip(df_ts["ts"], df_ts["id"]))

    records = []
    for _, row in df.iterrows():
        val = row["object_value"]
        obj = val if isinstance(val, dict) else json.loads(val)

        order_key = obj.get("_id")
        if not order_key:
            continue

        order_status = obj.get("final_status")
        if not order_status:
            continue

        rest_source_id = obj.get("restaurant", {}).get("id")
        if not rest_source_id or rest_source_id not in rest_map:
            continue
        surrogate_rest_id = rest_map[rest_source_id]

        user_source_id = obj.get("user", {}).get("id")
        if not user_source_id or user_source_id not in user_map:
            continue
        surrogate_user_id = user_map[user_source_id]

        raw_date = obj.get("date")
        if not raw_date:
            continue
        ts_value = pd.to_datetime(raw_date)
        ts_key = pd.Timestamp(ts_value).floor('s')
        if ts_key not in ts_map:
            continue
        surrogate_ts_id = ts_map[ts_key]

        records.append({
            "order_key": order_key,
            "order_status": order_status,
            "restaurant_id": surrogate_rest_id,
            "timestamp_id": surrogate_ts_id,
            "user_id": surrogate_user_id,
        })

    df_new = pd.DataFrame(records)
    if df_new.empty:
        log.warning("No orders to load; skipping.")
        return

    with database_engine.connect() as conn:
        existing = pd.read_sql(
            f"SELECT order_key FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new.merge(
        existing, on="order_key", how="left", indicator=True
    )
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new orders to insert.")

def load_fct_product_sales(stg_table_name: str, dds_table_name: str, **context):

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    with database_engine.connect() as conn:
        df_events = pd.read_sql(
            "SELECT event_ts, event_value "
            "FROM stg.bonussystem_events "
            "WHERE event_type = 'bonus_transaction' "
            "ORDER BY event_ts",
            con=conn
        )

    if df_events.empty:
        log.warning("No bonus_transaction events; skipping.")
        return

    bonus_by_order = {}
    for _, event_row in df_events.iterrows():
        ev = event_row["event_value"]
        data = ev if isinstance(ev, dict) else json.loads(ev)
        order_id = data.get("order_id")
        if order_id:
            bonus_by_order[order_id] = data.get("product_payments", [])

    with database_engine.connect() as conn:
        df_orders = pd.read_sql(
            "SELECT id, order_key FROM dds.dm_orders", con=conn
        )
        df_products = pd.read_sql(
            "SELECT id, product_name FROM dds.dm_products", con=conn
        )

    order_map = dict(zip(df_orders["order_key"], df_orders["id"]))
    prod_map = dict(zip(df_products["product_name"], df_products["id"]))

    records = []
    for order_key, product_payments in bonus_by_order.items():
        if not product_payments:
            continue

        order_id = order_map.get(order_key)
        if order_id is None:
            continue

        resolved_rows = []
        all_found = True

        for pp in product_payments:
            product_name = pp.get("product_name")
            if not product_name:
                all_found = False
                break

            product_id = prod_map.get(product_name)
            if product_id is None:
                all_found = False
                break

            resolved_rows.append({
                "product_id": product_id,
                "order_id": order_id,
                "count": pp.get("quantity", 0),
                "price": pp.get("price", 0),
                "total_sum": pp.get("product_cost", 0),
                "bonus_payment": pp.get("bonus_payment", 0),
                "bonus_grant": pp.get("bonus_grant", 0),
            })

        if not all_found:
            continue

        records.extend(resolved_rows)

    df_new = pd.DataFrame(records)
    if df_new.empty:
        log.warning("No product sales to load; skipping.")
        return

    with database_engine.connect() as conn:
        existing = pd.read_sql(
            f"SELECT product_id, order_id FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new.merge(
        existing, on=["product_id", "order_id"], how="left", indicator=True
    )
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new product sales to insert.")


# ==================== НОВОЕ ====================
def load_dm_couriers(stg_table_name: str, dds_table_name: str, **context):
    """Загрузка курьеров из stg.api_couriers в dds.dm_couriers"""

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)
    log.info(f"Read {len(df)} rows from stg.{stg_table_name}")

    if df.empty:
        log.warning("No couriers to load; skipping.")
        return

    records = []
    for _, row in df.iterrows():
        val = row["object_value"]
        obj = val if isinstance(val, dict) else json.loads(val)
        records.append({
            "courier_original_id": str(obj["_id"]),
            "courier_name": obj.get("name"),
        })

    df_new = pd.DataFrame(records)

    # Дедупликация внутри батча
    df_new = df_new.drop_duplicates(subset=["courier_original_id"], keep="first")

    # Дедупликация против базы
    with database_engine.connect() as conn:
        existing = pd.read_sql(
            f"SELECT courier_original_id FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new.merge(
        existing, on="courier_original_id", how="left", indicator=True
    )
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new couriers to insert.")


def load_fct_deliveries(stg_table_name: str, dds_table_name: str, **context):
    """Загрузка доставок из stg.api_deliveries в dds.fct_deliveries"""

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    database_engine = database_hook.get_sqlalchemy_engine()

    sql_query = f"SELECT object_id, object_value FROM stg.{stg_table_name}"
    with database_engine.connect() as conn:
        df = pd.read_sql(sql_query, con=conn)

    if df.empty:
        log.warning("No deliveries in STG; skipping.")
        return

    # Маппинги из DDS
    with database_engine.connect() as conn:
        df_orders = pd.read_sql("SELECT id, order_key FROM dds.dm_orders", con=conn)
        df_couriers = pd.read_sql("SELECT id, courier_original_id FROM dds.dm_couriers", con=conn)

    order_map = dict(zip(df_orders["order_key"], df_orders["id"]))
    courier_map = dict(zip(df_couriers["courier_original_id"], df_couriers["id"]))

    records = []
    for _, row in df.iterrows():
        val = row["object_value"]
        obj = val if isinstance(val, dict) else json.loads(val)

        delivery_id = obj.get("delivery_id")
        if not delivery_id:
            continue

        order_key = obj.get("order_id")
        if not order_key or order_key not in order_map:
            continue
        order_id = order_map[order_key]

        courier_source_id = obj.get("courier_id")
        if not courier_source_id or courier_source_id not in courier_map:
            continue
        courier_id = courier_map[courier_source_id]

        records.append({
            "delivery_original_id": delivery_id,
            "order_id": order_id,
            "courier_id": courier_id,
            "order_ts": pd.to_datetime(obj.get("order_ts")),
            "delivery_ts": pd.to_datetime(obj.get("delivery_ts")) if obj.get("delivery_ts") else None,
            "address": obj.get("address"),
            "rate": float(obj.get("rate")),
            "sum": obj.get("sum"),
            "tip_sum": obj.get("tip_sum"),
        })

    df_new = pd.DataFrame(records)
    if df_new.empty:
        log.warning("No deliveries to load; skipping.")
        return

    # Дедупликация против базы по delivery_original_id
    with database_engine.connect() as conn:
        existing = pd.read_sql(
            f"SELECT delivery_original_id FROM dds.{dds_table_name}",
            con=conn
        )

    df_new = df_new.merge(
        existing, on="delivery_original_id", how="left", indicator=True
    )
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_sql(
            name=dds_table_name,
            con=database_engine,
            schema="dds",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        log.info(f"Inserted {len(df_new)} rows into dds.{dds_table_name}.")
    else:
        log.info("No new deliveries to insert.")


def update_dm_orders_courier(**context):
    """Обновление courier_id в dds.dm_orders по данным из fct_deliveries"""

    database_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    conn = database_hook.get_conn()  # psycopg2-соединение, у него есть commit()
    cur = conn.cursor()

    cur.execute("""
        UPDATE dds.dm_orders o
        SET courier_id = f.courier_id
        FROM dds.fct_deliveries f
        WHERE o.id = f.order_id
          AND o.courier_id IS NULL
    """)

    conn.commit()
    log.info(f"Updated courier_id in dm_orders, rows affected: {cur.rowcount}")
    cur.close()
    conn.close()

# ===============================================


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_to_dds_new",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["dwh", "dds", 'usr'],
) as dag:

    task_load_users = PythonOperator(
        task_id="load_dm_users",
        python_callable=load_dm_users,
        op_kwargs={
            "stg_table_name": "ordersystem_users",
            "dds_table_name": "dm_users"
        },
    )

    task_load_restaurants = PythonOperator(
        task_id="load_dm_restaurants",
        python_callable=load_dm_restaurants,
        op_kwargs={
            "stg_table_name": "ordersystem_restaurants",
            "dds_table_name": "dm_restaurants"
        },
    )

    task_load_timestamps = PythonOperator(
        task_id="load_dm_timestamps",
        python_callable=load_dm_timestamps,
        op_kwargs={
            "stg_table_name": "ordersystem_orders",
            "dds_table_name": "dm_timestamps"
        },
    )

    task_load_products = PythonOperator(
        task_id="load_dm_products",
        python_callable=load_dm_products,
        op_kwargs={
            "stg_table_name": "ordersystem_orders",
            "dds_table_name": "dm_products"
        },
    )

    task_load_orders = PythonOperator(
        task_id="load_dm_orders",
        python_callable=load_dm_orders,
        op_kwargs={
            "stg_table_name": "ordersystem_orders",
            "dds_table_name": "dm_orders"
        },
    )

    task_load_fct_product_sales = PythonOperator(
        task_id="load_fct_product_sales",
        python_callable=load_fct_product_sales,
        op_kwargs={
            "stg_table_name": "ordersystem_orders",
            "dds_table_name": "fct_product_sales"
        },
    )

    # --- НОВЫЕ ТАСКИ ---
    task_load_couriers = PythonOperator(
        task_id="load_dm_couriers",
        python_callable=load_dm_couriers,
        op_kwargs={
            "stg_table_name": "api_couriers",
            "dds_table_name": "dm_couriers"
        },
    )

    task_load_fct_deliveries = PythonOperator(
        task_id="load_fct_deliveries",
        python_callable=load_fct_deliveries,
        op_kwargs={
            "stg_table_name": "api_deliveries",
            "dds_table_name": "fct_deliveries"
        },
    )

    task_update_dm_orders_courier = PythonOperator(
        task_id="update_dm_orders_courier",
        python_callable=update_dm_orders_courier,
    )

    # --- ЦЕПОЧКА ЗАВИСИМОСТЕЙ ---

    # Существующая цепочка
    (task_load_users, task_load_restaurants, task_load_timestamps, task_load_products) >> task_load_orders >> task_load_fct_product_sales

    # Доставки ждут и заказы, и курьеров
    [task_load_orders, task_load_couriers] >> task_load_fct_deliveries

    # Обновление courier_id в dm_orders — после доставок
    task_load_fct_deliveries >> task_update_dm_orders_courier

