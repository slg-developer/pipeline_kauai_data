import os
from datetime import datetime, timedelta
import pandas
import xlrd
import csv
import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks import wasb_hook

# from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from jinja2 import Environment, FileSystemLoader

#url for the csv file of kauai visitor data
KAUAI_VISITOR_URL = ''
#url for the file of Kauai county metrics.  It's in old school excel format
KAUAI_JOBS_DATA_URL = 'http://files.hawaii.gov/dbedt/economic/data_reports/mei/2020-05-kauai.xls'


def get_kauai_visitors():
    # this function GETs the latest file of kauai visitor data
    visitor_file = requests.get(KAUAI_VISITOR_URL)


def get_kauai_jobs():
    # this function GETs the latest file of kauai metrics from the county
    # website, converts it from xls to csv format, and writes it to Azure
    response = requests.get(KAUAI_JOBS_DATA_URL)
    # response.content is of type bytes which xlrd can't read direclty, so need
    # to save the file locally and open it with xlrd, unfortunatly.
    tKauaiJobs = 'kauai_jobs.xls'
    csvKauaiJobsOut = 'kauai_jobs_data.csv'
    open(tKauaiJobs, 'wb').write(response.content)
    wb = xlrd.open_workbook(tKauaiJobs)
    sh = wb.sheet_by_index(0)
    kauai_csv = open(csvKauaiJobsOut, 'w')
    wr = csv.writer(kauai_csv, quoting=csv.QUOTE_ALL)
    lHeaders = ['year','month','Arts_Entertainment_Recreation','Accommodation','FoodServices_DrinkingPlaces']
    wr.writerow(lHeaders)
    for rownum in range(6, sh.nrows-7):
        lDataRow = [
            sh.row_values(rownum)[0]\
            ,sh.row_values(rownum)[1]\
            ,sh.row_values(rownum)[20]\
            ,sh.row_values(rownum)[21]\
            ,sh.row_values(rownum)[22]
        ]
        wr.writerow(lDataRow)
    kauai_csv.close()
    # post csv to Azure blob storage
    vWASB = WasbHook(wasb_conn_id="azure_blob")
    container_name = "kauai_data"
    blob_name = csvKauaiJobsOut
    # file_path =”/home/xxx/xxxx.csv”
    wb.load_file(container_name, blob_name)
    # remove temp xls file
    os.remove(tKauaiJobs)
    os.remove(csvKauaiJobsOut)

def write_questions_to_s3():
    hook = S3Hook(aws_conn_id="s3_connection")
    hook.load_string(
        string_data=filter_questions(),
        key=S3_FILE_NAME,
        bucket_name=Variable.get("S3_BUCKET"),
        replace=True,
    )