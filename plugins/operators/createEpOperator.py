# BaseOperator
from airflow.models.baseoperator import BaseOperator

# Hooks
from hooks.s3Hook import MyS3Hook
from hooks.epHook import EpHook
from hooks.sftpHook import SFTPHook

from utils.preprocessor import (
    split_kst,
    extract_reserved_review
)

# Generals
import logging
import pandas as pd

# setup logging format
logger = logging.getLogger(__name__)

class CreateEpOperator(BaseOperator):
    """
        Operator: Get update dataset and create EP schema
        
        1. Read Update dataset
        2. Read Review dataset and apply with agent's review
        3. Create EP dataset with formed schema
        4. Upload EP dataset to server(cafe24) via SFTP and S3 Gold bucket
    """
    
    template_fields = (
        "target",
        "listed",
        "upload_path",
        "reserved",
        "review",
        "prefix",
        "gold_bucket",
        "silver_bucket",
        "bronze_bucket"
    )
    
    def __init__(
        self,
        bronze_bucket,
        silver_bucket,
        gold_bucket,
        prefix,
        review,
        reserved,
        upload_path,
        listed,
        target,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.bronze_bucket = bronze_bucket
        self.silver_bucket = silver_bucket
        self.gold_bucket = gold_bucket
        self.prefix = prefix
        self.review = review
        self.reserved = reserved
        self.upload_path = upload_path
        self.listed = listed
        self.target = target
        
    
    
    
    def execute(self, context):
        # Get data from S3 - updated 
        s3Hook = MyS3Hook()
        df = s3Hook.get_latest_parquet(self.silver_bucket, self.prefix)
        
        # Get data from S3 - review / reserved
        
        review = s3Hook.get_latest_parquet(self.silver_bucket, self.review)
        reserved = s3Hook.get_file(self.bronze_bucket, self.reserved)
        
        extracted_review = extract_reserved_review(review, reserved)
        
        # Create EP dataset
        epHook = EpHook(df=df, review=extracted_review, target=self.target)
        ep, duplicated = epHook.create_ep()
        
        ti = context["ti"]
        ti.xcom_push(
            key='removed_product_codes',
            value=duplicated
        )
        
        # Append Custom listed ad to ep
        listed_ep = s3Hook.get_file(self.silver_bucket, self.listed)
        listed_ep['naver_category'] = listed_ep['naver_category'].astype(str)
        listed_ep['model_number'] = listed_ep['model_number'].astype(str)
        
        finall_ep = pd.concat([ep, listed_ep])
        
        # Upload cafe24 via SFTP
        sftp = SFTPHook()
        sftp.upload_df(finall_ep, self.upload_path)
        
        
        # upload into S3 Gold bucket
        execution_date = context['logical_date']
        ymd, hm = split_kst(execution_date)
    
        new_key = f"{self.target}/{ymd}/{hm}/result.parquet"
        logger.info(
                "[READY] Updated parqeut is ready to be uploaded!",
                extra={
                    'event' : "Update Done"
                }
            )
            
        s3Hook.upload_file(finall_ep, self.gold_bucket, new_key)