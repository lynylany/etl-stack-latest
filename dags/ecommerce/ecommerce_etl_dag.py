from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ecommerce_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce", "dw", "superstore"],
) as dag:

    date_etl = BashOperator(
        task_id="dim_date_etl",
        bash_command="python /opt/airflow/dags/ecommerce/dim_date_etl.py",
    )

    geo_etl = BashOperator(
        task_id="dim_geography_etl",
        bash_command="python /opt/airflow/dags/ecommerce/dim_geography_etl.py",
    )

    cust_etl = BashOperator(
        task_id="dim_customer_etl",
        bash_command="python /opt/airflow/dags/ecommerce/dim_customer_etl.py",
    )

    prod_etl = BashOperator(
        task_id="dim_product_etl",
        bash_command="python /opt/airflow/dags/ecommerce/dim_product_etl.py",
    )

    fact_sales = BashOperator(
        task_id="fact_sales_etl",
        bash_command="python /opt/airflow/dags/ecommerce/fact_sales_etl.py",
    )

    # กำหนดลำดับ: Dimensions ต้องรันเสร็จทั้งหมดก่อน Fact Sales จึงจะเริ่มทำงาน
    [date_etl, geo_etl, cust_etl, prod_etl] >> fact_sales