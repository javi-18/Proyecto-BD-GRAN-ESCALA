from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_PATH = "/mnt/c/Users/javie/OneDrive/Escritorio/BD GRAN ESCALA/proyecto"

SILVER_FILE = f"{PROJECT_PATH}/data/silver/sales_silver.parquet"
DBT_PATH = f"{PROJECT_PATH}/dbt_project"


default_args = {
    "owner": "data-engineering-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="global_mart_consolidation_pipeline",
    description="Pipeline local Super-Market: Beam, Silver, PostgreSQL y dbt",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    tags=["super-market", "beam", "dbt", "postgres"],
) as dag:

    extract_and_transform_silver = BashOperator(
        task_id="extract_and_transform_silver",
        bash_command=f"""
        cd "{PROJECT_PATH}" &&
        python3 beam/pipeline.py
        """,
    )

    sensor_silver_data = BashOperator(
        task_id="sensor_silver_data",
        bash_command=f"""
        test -f "{SILVER_FILE}"
        """,
    )

    load_silver_to_postgres = BashOperator(
        task_id="load_silver_to_postgres",
        bash_command=f"""
        cd "{PROJECT_PATH}" &&
        /mnt/c/Python312/python.exe dbt_project/load_silver_to_postgres.py
        """,
    )

    load_and_model_gold = BashOperator(
    task_id="load_and_model_gold",
    bash_command=f"""
    cd "{DBT_PATH}" &&
    dbt seed --profiles-dir . &&
    dbt run --profiles-dir . &&
    dbt test --profiles-dir .
    """,
    )

    extract_and_transform_silver >> sensor_silver_data >> load_silver_to_postgres >> load_and_model_gold