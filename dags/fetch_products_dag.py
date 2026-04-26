from airflow import DAG

# Operators
from operators.fetchApiOperator import FetchAPIOperator
from operators.createEpOperator import CreateEpOperator
from operators.saveLogOperator import SaveLogOperator
from airflow.operators.empty import EmptyOperator

# Log
from configs.logging_config import setup_logging

setup_logging()

# Extra.  
from configs.urls import (
        URLS,
        COOKIE_URL,        
)
from configs.payloads import (
    PTNCD,
)
from configs.s3_config import (
    BRONZE_BUCKET,
    SILVER_BUCKET,
    GOLD_BUCKET,
    RAW_DATA_KEY,
    UPDATED_FOLDER,
    REVIEW_FOLDER,
    REVIEW_SEJU,
    REVIEW_DREAM,
    LOG_FOLDER,
    SEJU_LISTED,
    HOPE_LISTED,
)
from utils.slack_alert import (
    on_success,
    on_failure
)
from configs.sftp import (
    SFTP_PATH_SEJU,
    SFTP_PATH_DREAM
)


# Time Zone -> KST
import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Seoul")



# General
import os

local_dir = os.getenv("raw_data")


env = {
    'local_dir' : local_dir,
}

with DAG(
    dag_id="fetch_products",
    catchup=False,
    start_date=datetime(2026,1,1, tzinfo=local_tz),
    schedule="0 9,10,11,12,13,14,15,16,17 * * *",
    tags=['test', 'EP'],
    on_success_callback=on_success,
    on_failure_callback=on_failure,
):
    
    start = EmptyOperator(task_id="start")
    
    fetch_data = FetchAPIOperator(
        task_id="fetch_api",
        bronze_bucket=BRONZE_BUCKET,
        silver_bucket=SILVER_BUCKET,
        raw_key=RAW_DATA_KEY,
        folder=UPDATED_FOLDER,
        env=env,
        urls=URLS,
        cookie_url=COOKIE_URL,
        ptnCd=PTNCD,
    )
    
    create_ep_seju = CreateEpOperator(
        task_id="create_ep_seju",
        bronze_bucket=BRONZE_BUCKET,
        silver_bucket=SILVER_BUCKET,
        gold_bucket=GOLD_BUCKET,
        prefix=UPDATED_FOLDER,
        review=REVIEW_FOLDER,
        reserved=REVIEW_SEJU,
        upload_path=SFTP_PATH_SEJU,
        listed=SEJU_LISTED,
        target='seju'
    )
    
    
    create_ep_dream = CreateEpOperator(
        task_id="create_ep_dream",
        bronze_bucket=BRONZE_BUCKET,
        silver_bucket=SILVER_BUCKET,
        gold_bucket=GOLD_BUCKET,
        prefix=UPDATED_FOLDER,
        review=REVIEW_FOLDER,
        reserved=REVIEW_DREAM,
        upload_path=SFTP_PATH_DREAM,
        listed=HOPE_LISTED,
        target='hope'
    )
    
    
    save_logs = SaveLogOperator(
        task_id='Exclude_logs',
        gold_bucket=GOLD_BUCKET,
        prefix=LOG_FOLDER
    )
    
    
    end = EmptyOperator(task_id="end")
    
    start >> fetch_data >> create_ep_seju >> create_ep_dream >> save_logs >> end
    
    