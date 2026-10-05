from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="environmental_etl",
    start_date=datetime(2026, 9, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["environmental", "etl"],
) as dag:

    area_etl = BashOperator(
        task_id="area_etl",
        bash_command="python /opt/airflow/dags/area_etl.py",
    )

    station_etl = BashOperator(
        task_id="station_etl",
        bash_command="python /opt/airflow/dags/station_etl.py",
    )

    aqi_etl = BashOperator(
        task_id="aqi_etl",
        bash_command="python /opt/airflow/dags/aqi_etl.py",
    )

    weather_etl = BashOperator(
        task_id="weather_etl",
        bash_command="python /opt/airflow/dags/weather_etl.py",
    )

    area_etl >> station_etl
    station_etl >> [aqi_etl, weather_etl]