from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from uniprot_ingestion import ingest_uniprot_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'uniprot_ingestion',
    default_args=default_args,
    description='UniProt data ingestion pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

ingest_data_task = PythonOperator(
    task_id='ingest_uniprot_data',
    python_callable=ingest_uniprot_data,
    dag=dag
)
