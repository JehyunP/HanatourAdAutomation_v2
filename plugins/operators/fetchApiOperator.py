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


class FetchAPIOperator(BaseOperator):
    """
        Operator: Fetch data from API endpoint(XHR)
        
        Using APIFetchHook
        1. Check the product is available 
        2. Get current price 
        to update product information
        
        Then upload on S3 (silver)
        
    """
    
    template_fields = (
        "thread_n", 
        "ptnCd", 
        "cookie_url", 
        "urls", 
        "env", 
        "folder",
        "raw_key", 
        "silver_bucket", 
        "bronze_bucket"
    )
    
    def __init__(
        self,
        bronze_bucket = '',
        silver_bucket = '',
        raw_key = '',
        folder = '',
        env = '',
        urls = [],
        cookie_url = '',
        ptnCd = '',
        thread_n=10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bronze_bucket = bronze_bucket
        self.silver_bucket = silver_bucket
        self.raw_key = raw_key
        self.folder = folder
        self.env = env
        self.urls = urls
        self.cookie_url = cookie_url
        self.ptnCd = ptnCd
        self.thread_n = thread_n
        

    def execute(self, context):
        
        # containers
        data_set = []
        errors = defaultdict(list)
        
        # Open dataFrame
        s3_hook = MyS3Hook()

        # Session Create -> Cookie based
        apiHook = APIHook(self.cookie_url, self.ptnCd)
        
        try:
            # Fetch API Pipeline initiate
            df = s3_hook.get_file(self.bronze_bucket, self.raw_key)
            run_list = df["product_code"].dropna().astype(str).tolist()
                
            # multi-Threading -> chech product seat -> update price 
            with ThreadPoolExecutor(max_workers=int(self.thread_n)) as executor:
                futures = [
                    executor.submit(
                        apiHook.run_pipeline,
                        self.urls,
                        item
                    ) for item in run_list
                ]
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception as e:
                        logger.error(
                            "Thread execution failed",
                            extra={
                                'event' : "Thread"
                            }
                        )
                        errors["unexpected"].append(str(e))
                        continue
                    
                    data_set.append(result[0])
                    
                    if result[1]:
                        errors['status_check_error'].append(result[1])
                    if result[2]:
                        errors['product_disable'].append(result[2])
                    if result[3]:
                        errors['no_seat'].append(result[3])
                        
            logger.info(
                "Fetch API task finished. success=%s, errors=%s",
                len(data_set),
                dict(errors),
            )
            
            # drop empty data in dataset then conver to df
            data_set = [data for data in data_set if data]
            data_df = pd.DataFrame(data_set) 
            
            # join df and updated dataset by product_code
            new_df = pd.merge(df, data_df, how='left', on='product_code')
            
            # drop non-updated rows
            new_df = new_df[new_df['adtTotlAmt'].notna()]
            
            # Save new data frame into S3 silver bucket
            execution_date = context['logical_date']
            ymd, hm = split_kst(execution_date)
            
            new_key = f"{self.folder}/{ymd}/{hm}/updated.parquet"
            
            logger.info(
                "[READY] Updated parqeut is ready to be uploaded!",
                extra={
                    'event' : "Update Done"
                }
            )
            
            s3_hook.upload_file(new_df, self.silver_bucket, new_key)
            
            
            
        finally:
            apiHook.close()
            
        
        # Send Error lists to Xcom -> Will shoot Slack Alert once the entire job ends
        return errors
