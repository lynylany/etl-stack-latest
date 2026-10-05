"""
ตัวอย่าง DAG สำหรับสอน ETL บน Airflow 3.x
Extract (สร้างข้อมูลจำลอง) -> Transform (pandas) -> Load (PostgreSQL: etl_db)

จุดต่างจาก Airflow 2:
- import จาก airflow.sdk (Task SDK ใหม่ของ Airflow 3) แทน airflow / airflow.operators
- ใช้ @dag / @task decorator ส่งข้อมูลระหว่าง task ด้วย return value (XCom อัตโนมัติ)
"""
from datetime import datetime, timedelta
import random

from airflow.sdk import dag, task

CONN_ID = "etl_postgres"  # ถูก preload ไว้ใน docker-compose แล้ว


@dag(
    dag_id="example_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["teaching", "etl"],
)
def example_etl_pipeline():

    @task
    def extract() -> list[dict]:
        products = ["notebook", "mouse", "keyboard", "monitor"]
        return [
            {
                "product": random.choice(products),
                "quantity": random.randint(1, 10),
                "price": round(random.uniform(100, 5000), 2),
            }
            for _ in range(20)
        ]

    @task
    def transform(rows: list[dict]) -> list[dict]:
        import pandas as pd

        df = pd.DataFrame(rows)
        df["total"] = df["quantity"] * df["price"]   # ตัวอย่าง transform ง่าย ๆ
        df = df[df["quantity"] > 2]                  # filter ตัวอย่าง
        return df.drop(columns=["total"]).to_dict("records")

    @task
    def load(rows: list[dict]) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=CONN_ID)
        hook.insert_rows(
            table="sales",
            rows=[(r["product"], r["quantity"], r["price"]) for r in rows],
            target_fields=["product", "quantity", "price"],
        )

    load(transform(extract()))


example_etl_pipeline()
