from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "S.T - DE",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

spark_script = "/opt/airflow/src/ecommerce_medallion_pipeline.py"
jdbc_jar = "/opt/airflow/src/jars/postgresql-42.6.0.jar"

with DAG(
    dag_id="ecommerce_medallion_etl",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["medallion", "spark", "ecommerce"],
) as dag:

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"spark-submit --jars {jdbc_jar} {spark_script} --layer bronze"
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=f"spark-submit --jars {jdbc_jar} {spark_script} --layer silver"
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"spark-submit --jars {jdbc_jar} {spark_script} --layer gold"
    )

    bronze >> silver >> gold
