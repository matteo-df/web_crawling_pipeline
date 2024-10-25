from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from dotenv import load_dotenv
import os
import logging

from include.warc_utils import save_warc_file_names_as_csv, get_warc_file_chunks, process_warc_file

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

default_args = {
  'owner': 'matteo'
}

# Define the DAG
with DAG(
  dag_id='warc_process_dag',
  default_args=default_args,
  start_date=datetime(2024, 1, 1),
  schedule='@hourly',
  is_paused_upon_creation=True,
  catchup=False
) as dag:

  warc_file_sensor = FileSensor(
    task_id='check_for_warc_file',
    filepath='/opt/airflow/dags/data/warc/*',
    poke_interval=120,
    timeout=600
  )

  # Task to list and split files
  get_warc_files = PythonOperator(
    task_id='get_warc_files',
    python_callable=save_warc_file_names_as_csv,
    op_args=['warc'],
  )

  spark_data_processing_task = SparkSubmitOperator(
    task_id='spark_data_processing_task',
    application='/opt/airflow/dags/include/data_processing.py',
    conn_id='spark-master',
    verbose=True,
    name='warc_processing_task',
    application_args=[],
    jars='/opt/postgresql-42.7.4.jar',
    dag=dag,
  )

  # Create tasks to process each file chunk in parallel
  def create_warc_processing_tasks():
    task_parallelism = 2 #int(os.getenv('WARC_PARALLELISM'))
    warc_chunks=get_warc_file_chunks(task_parallelism)
    logger.info(warc_chunks)
    for i in range(task_parallelism):
      try:
        logger.info(warc_chunks[i])
        task = PythonOperator(
          task_id=f'process_warc_chunk_{i+1}',
          python_callable=process_warc_file,
          op_args=[warc_chunks[i]],
        )
        warc_file_sensor >> get_warc_files >> task >> spark_data_processing_task
      except:
        print('All chunks already processed')

  # Create tasks for processing file chunks in parallel
  create_warc_processing_tasks()


  
