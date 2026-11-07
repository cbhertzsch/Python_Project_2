
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="etl_spark_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["project2"],
) as dag:

    spark_connection_test = BashOperator(
        task_id="spark_connection_test",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.driver.host=project2-airflow \
          --conf spark.driver.bindAddress=0.0.0.0 \
          --conf spark.executor.memory=1g \
          --conf spark.cores.max=1 \
          /opt/airflow/scripts/test_spark.py
        """,
    )

    completion_test = EmptyOperator(
        task_id="completion_test"
    )

    spark_connection_test >> completion_test
