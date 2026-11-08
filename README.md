# Python Project 2 – ETL Data Pipeline

## Project Overview

For Project 2, I created an ETL (Extract, Transform, Load) data pipeline using Python, Apache Airflow, Apache Spark (PySpark), PostgreSQL, and Docker. The purpose of this project was to generate a large amount of sample data, clean data-quality issues, perform analytics, and load the results into a PostgreSQL database.

Apache Airflow was used to organize and automate the different steps of the pipeline. Apache Spark was used for data cleansing and analytics, while PostgreSQL stored the cleaned data and results.

## Tools Used

- **Python and Faker:** Generated sample customer, product, and order data.
- **Docker and Docker Compose:** Ran the project services in containers.
- **Apache Airflow:** Scheduled and organized the ETL workflow.
- **Apache Spark (PySpark):** Cleaned, validated, and analyzed the data.
- **PostgreSQL:** Stored cleaned records, analytics results, and audit information.
- **Pandas:** Read Parquet files and prepared records for PostgreSQL.
- **Git and GitHub:** Tracked project changes and stored the code.

## Data Generation

I used Python and Faker to generate 100,000 customer records, 100,000 product records, and 100,000 order records.

I also added 20 duplicate records to each dataset and introduced data-quality issues to test the cleansing process.

The generated data was saved in CSV, JSON, and YAML formats.

| Dataset | Raw Records | Cleaned Records | Removed |
|---|---:|---:|---:|
| Customers | 100,020 | 99,990 | 30 |
| Products | 100,020 | 99,985 | 35 |
| Orders | 100,020 | 99,949 | 71 |
| **Total** | **300,060** | **299,924** | **136** |

## Data Cleansing

I created three separate PySpark cleansing scripts for customers, products, and orders.

The cleansing process included removing duplicate records, checking missing values, correcting capitalization and whitespace, validating product prices and order amounts, standardizing dates, and checking that orders referenced valid customers and products.

The cleaned datasets were saved in Parquet format under `data/cleaned/`.

I also created a data-quality logging script that records the raw and cleaned record counts, how many records were removed, and the reasons for removal. The summary is saved to `data/logs/cleansing_summary.txt`.

## Data Analytics

After cleansing, I used PySpark to calculate 13 analytics datasets:

1. Customer analytics
2. Product analytics
3. Category performance
4. Average product price
5. Inventory status
6. Daily sales
7. Monthly sales
8. Order status summary
9. Payment method summary
10. Discount summary
11. Revenue by customer type
12. Categories by customer type
13. City revenue

The analytics results were saved as Parquet files under `data/analytics/` and then loaded into PostgreSQL.

## PostgreSQL Database

I created 17 PostgreSQL tables consisting of three cleaned data tables, 13 analytics tables, and one ETL audit table.

The database includes primary keys and foreign-key relationships to maintain data integrity.

The loading script refreshes the cleaned and analytics tables using the latest Parquet files. The audit table stores information about successful database loads, including timestamps, status, and the number of records processed.

## Apache Airflow Workflow

I created a DAG named `etl_spark_pipeline` to organize the project into 10 tasks.

The tasks run in this order:

1. `generate_data`
2. `file_validation`
3. `spark_cleanse_customers`
4. `spark_cleanse_products`
5. `spark_cleanse_orders`
6. `cleansing_log`
7. `spark_analytics`
8. `load_to_postgres`
9. `data_validation`
10. `completion`

The DAG is configured to run daily and can also be triggered manually through the Airflow web interface.

## Project Setup

The project uses Docker Compose to run Apache Airflow, Spark, and PostgreSQL.

To start the environment from the project folder:

```bash
docker compose up -d
```

After the containers are running, the web interfaces are available at:

- **Airflow:** http://localhost:8088
- **Spark Master:** http://localhost:8080
- **Spark Worker:** http://localhost:8081

The project uses environment variables for PostgreSQL settings. A `.env.example` file is included as a reference. Actual credentials and generated data files are excluded from GitHub.

After configuring the environment and initializing the database using `sql/schema.sql`, the Airflow DAG can be triggered to generate, cleanse, analyze, and load the data.

## Testing and Validation

I tested the cleansing scripts separately before connecting them to Airflow.

I also verified that the cleaned data contained the expected number of records, checked that invalid and duplicate records were removed, and confirmed that orders referenced valid customer and product IDs.

I tested PostgreSQL loading and confirmed the expected records were inserted into the database.

Finally, I ran the complete Airflow workflow and confirmed that all 10 tasks finished successfully.

## Project Results

The completed pipeline processed 300,060 raw records and produced 299,924 cleaned records.

The project demonstrated how Python, Airflow, Spark, and PostgreSQL can work together to automate data generation, cleansing, analytics, and database loading.

It also helped me understand how to use Docker containers, troubleshoot processing issues, and verify that each part of a data pipeline works correctly.