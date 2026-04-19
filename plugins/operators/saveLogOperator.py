from airflow.models.baseoperator import BaseOperator

# Hooks
from hooks.s3Hook import MyS3Hook

# Generals
from utils.preprocessor import split_kst
import pandas as pd
import logging 

# setup logging format
logger = logging.getLogger(__name__)


class SaveLogOperator(BaseOperator):
    """
        Save logs -> Product code of excluded products -> s3 gold bucket
    """
    
    template_fields = (
        "prefix",
        "gold_bucket"
    )
    
    def __init__(
        self, 
        gold_bucket, 
        prefix, 
        **kwargs
    ):
        super().__init__(**kwargs)
        self.gold_bucket = gold_bucket
        self.prefix = prefix
        
    
    def execute(self, context):
        # Get Xcom
        ti = context["task_instance"]
        execution_date = context.get("logical_date")
        
        errors = ti.xcom_pull(key="return_value") or {}

        status_check_error = errors.get("status_check_error", [])
        product_disable = errors.get("product_disable", [])
        no_seat = errors.get("no_seat", [])
        
        duplicated = ti.xcom_pull(
            task_ids="create_ep_task_id",
            key="removed_product_codes"
        ) or []
        
        error_report_df = pd.DataFrame({
            "duplicated": pd.Series(duplicated),
            "status_check_error": pd.Series(status_check_error),
            "product_disable": pd.Series(product_disable),
            "no_seat": pd.Series(no_seat),
        })

        # Save into S3 as xlsx
        ymd, hm = split_kst(execution_date)
        key = f'{self.prefix}/{ymd}/{hm}/excluded.xlsx'
        
        logger.info(
            "[READY] Updated parqeut is ready to be uploaded!",
            extra={
                'event' : "Update Done"
            }
        )
        s3_hook = MyS3Hook()
        s3_hook.upload_file(error_report_df, self.gold_bucket, key)
        