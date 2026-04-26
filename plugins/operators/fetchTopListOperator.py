# Import Base Operator (to inherit)
from airflow.models.baseoperator import BaseOperator

# Import hooks
from hooks.topListHook import TopListHook
from hooks.s3Hook import MyS3Hook

# Generals
import pandas as pd
import logging
from datetime import datetime, timedelta

from utils.preprocessor import split_kst
from utils.session import Session

# setup logging format
logger = logging.getLogger(__name__)



class FetchTopListOperator(BaseOperator):
    """
        Implements Version 1
        Check Top 10 dataset for each product codes
        Gathering meaningful factors to help building new title of ADs
        
        1. Check Travels Area 
        2. Gathering codes with heap queue - selecting top 10 packages
        3. Within each package, gathering, all the info contained 
        4. Collect and Combine all data into one dataframe and save into S3
    """
    
    template_fields = (
        "gold_bucket",
        "folder",
        "cookie_url",
        "urls"
    )
    
    def __init__(
        self,
        urls : list,
        cookie_url : str,
        folder : str,
        gold_bucket : str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.urls = urls
        self.cookie_url = cookie_url
        self.folder = folder
        self.gold_bucket = gold_bucket
        
        
    def execute(self, context):
        execution_date = context['logical_date']
        kst_date = execution_date.in_timezone("Asia/Seoul")
        
        session = Session(self.cookie_url)

        hook = TopListHook(self.urls, session)
        
        result = hook.run_pipeline(
            date=kst_date,
            number_thread=10
        )            
        
        yymmdd = kst_date.strftime("%Y%m%d")
        hhmm = kst_date.strftime('%H%M')
        
        key = f"{self.folder}/{yymmdd}/{hhmm}/result.parquet"
        
        s3Hook = MyS3Hook()
        s3Hook.upload_file(result, self.gold_bucket, key)
        