from airflow import DAG

# Operators
from operators.fetchReviewOperator import FetchReviewOperator
from airflow.operators.empty import EmptyOperator

# Log
from configs.logging_config import setup_logging

setup_logging()

# Extra.  
from configs.urls import (
    REVIEW_URLS,  
    COOKIE_URL      
)

from configs.s3_config import (
    SILVER_BUCKET,
    REVIEW_FOLDER
)

# Time Zone -> KST
import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="review_dag",
    catchup=False,
    start_date=datetime(2026,1,1, tzinfo=local_tz),
    schedule="@monthly",
    tags=['review'],
):
    
    start = EmptyOperator(task_id="start")

    
    fetch_review = FetchReviewOperator(
        task_id="fetch_api",
        silver_bucket=SILVER_BUCKET,
        folder=REVIEW_FOLDER,
        urls=REVIEW_URLS,
        cookie_url=COOKIE_URL,
    )
    
    end = EmptyOperator(task_id="end")    
    
    start >> fetch_review >> end