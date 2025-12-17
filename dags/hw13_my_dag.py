from datetime import datetime
from airflow.models.dag import DAG
from airflow.sdk.definitions.asset import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

cleaned_data_asset = Asset(uri="postgres://traindb:5432/traindb/public/clean_data_transactions")
start_date = datetime(2025, 10, 11)
with DAG(
    dag_id="create_cleaned_data_table",
    start_date=start_date,
    schedule=None,
    catchup=False
) as dag:
    task = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgresql_conn",
        sql=f"""drop table if exists clean_data_transactions cascade;
                create table clean_data_transactions(
                STORE_ID text,
                STORE_LOCATION text,
                PRODUCT_CATEGORY text,
                PRODUCT_ID text,
                MRP float,
                CP float,
                DISCOUNT float,
                SP float,
                Date text  
                );
                """,
        outlets=[cleaned_data_asset])
