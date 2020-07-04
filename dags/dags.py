from datetime import datetime, timedelta

from airflow import DAG
# don't need this guy after all
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils import get_kauai_visitors, get_kauai_jobs

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 9),
    "email": ["my_email@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
}

# start DAG definition
with DAG("kauai_data", default_args=default_args) as dag:

    # just grab file using direct url instead
    # tGetVisitorCounts = GCSToGCSOperator(
    #     task_id="get_visitors",
    #     source_bucket=VISITORS_SOURCE_BUCKET,
    #     source_object=VISITORS_SOURCE_OBJECT,
    #     destination_bucket=BUCKET_1_DST,  
    #     destination_object="backup_" + OBJECT_1 
    # )

    tGetVisitorCounts = PythonOperator(
        task_id="tGetVisitorCounts", python_callable=get_kauai_visitors
    )

    tGetJobCounts = PythonOperator(
        task_id="tGetJobCounts", python_callable=get_kauai_jobs
    )

    tCallSpark = SparkSubmitOperator(
        application="${SPARK_HOME}/examples/src/main/python/pi.py",
        task_id="submit_spark_job"
    )

    tWriteResults = PythonOperator(
        task_id="write_questions_to_s3", python_callable=write_questions_to_s3
    )

tGetVisitorCounts >> tGetJobCounts >> tCallSpark >> tWriteResults