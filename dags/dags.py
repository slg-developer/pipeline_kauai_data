from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils import get_kauai_visitors, get_kauai_jobs

default_args = {
    "owner": "Ranjeet",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 9),
    "email": ["my_email@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@monthly"
}

# start DAG definition
with DAG("kauai_data", default_args=default_args) as dag:

    # this file is manually uploaded to Azure Blob storage for now.
    # Make this better in the future by scraping the table in the website
    # and creating csv file
    # tGetVisitorCounts = PythonOperator(
    #     task_id="tGetVisitorCounts", python_callable=get_kauai_visitors
    # )

    tGetJobCounts = PythonOperator(
        task_id="tGetJobCounts", python_callable=get_kauai_jobs
    )

    # tCallSpark = SparkSubmitOperator(
    #     application="${SPARK_HOME}/examples/src/main/python/pi.py",
    #     task_id="submit_spark_job"
    # )

    # tWriteResults = PythonOperator(
    #     task_id="write_questions_to_s3", python_callable=write_questions_to_s3
    # )

# tGetVisitorCounts >> tGetJobCounts >> tUnpivotJobs >> tCallSpark >> tWriteResults
tGetJobCounts