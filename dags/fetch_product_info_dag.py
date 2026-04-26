from airflow import DAG

# Operators
from operators.fetchTopListOperator import FetchTopListOperator
from airflow.operators.empty import EmptyOperator

# Log
from configs.logging_config import setup_logging

setup_logging()

# Extra.  
from configs.urls import (
    REVIEW_URLS,
    COOKIE_URL,        
)

from configs.s3_config import (
    PRODUCT_INFO,
    GOLD_BUCKET
)

# Time Zone -> KST
import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="fetch_products_info",
    catchup=False,
    start_date=datetime(2026,1,1, tzinfo=local_tz),
    schedule="0 4 * * *",
    tags=['product-info', 'result'],
):
    
    start = EmptyOperator(task_id="start")

    fetch_product = FetchTopListOperator(
        task_id="fetch_top_products",
        urls=REVIEW_URLS,
        cookie_url=COOKIE_URL,
        folder=PRODUCT_INFO,
        gold_bucket=GOLD_BUCKET
    )
    
    end = EmptyOperator(task_id="end")

    start >> fetch_product >> end