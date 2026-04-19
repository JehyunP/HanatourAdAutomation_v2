# Import Base Operator (to inherit)
from airflow.models.baseoperator import BaseOperator

# Import hooks
from hooks.apiHook import APIHook
from hooks.s3Hook import MyS3Hook

# Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

# Generals
import pandas as pd
from collections import defaultdict
import logging

from utils.preprocessor import split_kst

# setup logging format
logger = logging.getLogger(__name__)


class FetchReviewOperator(BaseOperator):
    """
        Operator: Fetch review from API endpoints(XHR)
        
        Using APIFetchHook
        1. Get all the travel-areas
        2. Get all the review via checking reprsCd
        3. Update into S3 (silver)
    """
    
    template_fields = (
        "thread_n", 
        "cookie_url", 
        "urls", 
        "folder",
        "silver_bucket", 
    )
    
    def __init__(
        self,
        silver_bucket = '',
        folder = '',
        urls = [],
        cookie_url = '',
        thread_n=10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.silver_bucket = silver_bucket
        self.folder = folder
        self.urls = urls
        self.cookie_url = cookie_url
        self.thread_n = thread_n


    def execute(self, context):
        
        # Call api hook
        apiHook = APIHook(self.cookie_url)
        
        # execution date
        execution_date = context['logical_date']
        
        try:
            review_df = apiHook.run_pipeline_review(self.urls, execution_date)
            
            # Save into S3 as parquet
            ymd, hm = split_kst(execution_date)
            key = f'{self.folder}/{ymd}/{hm}/review.parquet'
            
            logger.info(
                "[READY] Updated parqeut is ready to be uploaded!",
                extra={
                    'event' : "Update Done"
                }
            )
            s3_hook = MyS3Hook()
            s3_hook.upload_file(review_df, self.silver_bucket, key)
            
            logger.info("review_df rows=%d cols=%d", review_df.shape[0], review_df.shape[1])
            logger.info("review_df memory=%d bytes", review_df.memory_usage(deep=True).sum())
            
        finally:
            apiHook.close()
            
