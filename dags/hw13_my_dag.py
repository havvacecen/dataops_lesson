from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import ObjectStoragePath, Asset, asset
import requests
from airflow.providers.ssh.operators.ssh import SSHOperator


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
                PRODUCT_ID integer,
                MRP float,
                CP float,
                DISCOUNT float,
                SP float,
                Date_Casted date  
                );
                """,
        outlets=[cleaned_data_asset])



OBJECT_STORAGE_SYSTEM = "s3"
OBJECT_STORAGE_CONN_ID = "s3_conn"
OBJECT_STORAGE_PATH = "dataops-bronze/raw"

@asset(
    schedule=[cleaned_data_asset],
)
def upload_dataset_to_minio():
    """
    Veri githubdan indiriliyor ve minioya yükleniyor
    """
    object_storage_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )
    target_file = object_storage_path / "dirty_store_transactions.csv"

    if target_file.exists():
        print("file already exists so this step is skipped")
        return

    response = requests.get("https://raw.githubusercontent.com/erkansirin78/datasets/refs/heads/master/dirty_store_transactions.csv")
    if response.status_code == 200:
        object_storage_path.mkdir(parents=True, exist_ok=True)
        target_file.write_bytes(response.content)





@asset(
    schedule=[upload_dataset_to_minio],
    uri="postgres://traindb:5432/traindb/public/clean_data_transactions" 
)
def transform_and_writePostgres():
    """
    Spark Client container'ın ve data_transform.py'yi çalıştırması sağlanıyor.
    """
    return SSHOperator(
        task_id="transform_and_writePosgres_data",
        ssh_conn_id="spark_client_connection",
        command="spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.0,org.postgresql:postgresql:42.2.18 /opt/spark_code/data_transform.py 2>&1",
        cmd_timeout=600
    )