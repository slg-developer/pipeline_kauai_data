import json
import os
from datetime import datetime, timedelta

import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from jinja2 import Environment, FileSystemLoader

#url for the csv file of kauai visitor data
KAUAI_VISITOR_URL = ''
#url for the file of Kauai county metrics.  It's in old school excel format
KAUAI_DATA_URL = 'http://files.hawaii.gov/dbedt/economic/data_reports/mei/2020-05-kauai.xls'


def get_kauai_visitors():
    # this function GETs the latest file of kauai visitor data
    visitor_file = requests.get(KAUAI_VISITOR_URL)


def get_kauai_jobs():
    # this function GETs the latest file of kauai metrics from the county website
    metrics_file = requests.get(KAUAI_DATA_URL)


def write_questions_to_s3():
    hook = S3Hook(aws_conn_id="s3_connection")
    hook.load_string(
        string_data=filter_questions(),
        key=S3_FILE_NAME,
        bucket_name=Variable.get("S3_BUCKET"),
        replace=True,
    )