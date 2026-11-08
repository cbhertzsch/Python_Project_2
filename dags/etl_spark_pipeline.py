
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Spark command settings
spark_command = (
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--deploy-mode client "
    "--conf spark.driver.host=project2-airflow "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.executor.memory=1g "
    "--conf spark.cores.max=1 "
)

with DAG(
    dag_id="etl_spark_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["project2"],
) as dag:

    # Generate the raw customer, product, and order data
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/generate_data.py"
    )

    # Check that all three raw files exist
    file_validation = BashOperator(
        task_id="file_validation",
        bash_command="""
        test -f /data/raw/customers.csv &&
        test -f /data/raw/products.csv &&
        test -f /data/raw/orders.csv
        """
    )

    
    # Clean customers using PySpark
    spark_cleanse_customers = BashOperator(
        task_id="spark_cleanse_customers",
        bash_command=spark_command +
        " /opt/airflow/scripts/spark_cleanse_customers.py customers"
    )

    # Clean products using PySpark
    spark_cleanse_products = BashOperator(
        task_id="spark_cleanse_products",
        bash_command=spark_command +
        " /opt/airflow/scripts/spark_cleanse_products.py products"
    )

    # Clean orders using the cleaned customer and product data
    spark_cleanse_orders = BashOperator(
        task_id="spark_cleanse_orders",
        bash_command=spark_command +
        " /opt/airflow/scripts/spark_cleanse_orders.py"
    )
    
    # Save the data-quality cleansing summary
    cleansing_log = BashOperator(
        task_id="cleansing_log",
        bash_command=spark_command +
        " /opt/airflow/scripts/cleansing_log.py"
    )

    # Calculate analytics using PySpark
    spark_analytics = BashOperator(
        task_id="spark_analytics",
        bash_command=spark_command +
        " /opt/airflow/scripts/spark_analytics.py"
    )

    # Load cleaned data and analytics into PostgreSQL
    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/scripts/load_postgres.py"
    )

    
    # Validate the loaded database records
    data_validation = BashOperator(
        task_id="data_validation",
        bash_command="python /opt/airflow/scripts/validate_postgres.py"
    )

    # Mark the pipeline as complete
    completion = EmptyOperator(
        task_id="completion"
    )

    
    # Run the tasks in the correct order
    generate_data >> file_validation
    file_validation >> spark_cleanse_customers
    spark_cleanse_customers >> spark_cleanse_products
    spark_cleanse_products >> spark_cleanse_orders
    spark_cleanse_orders >> cleansing_log
    cleansing_log >> spark_analytics
    spark_analytics >> load_to_postgres
    load_to_postgres >> data_validation >> completion


