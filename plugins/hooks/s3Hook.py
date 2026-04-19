# Hook
from airflow.hooks.base_hook import BaseHook

# Generals
import os
import boto3
import logging
import pandas as pd
import io
from pathlib import Path


# setup logging format
logger = logging.getLogger(__name__)


class MyS3Hook(BaseHook):
    
    def __init__(self):
        super().__init__()
        self.region_name = os.getenv("AWS_DEFAULT_REGION")
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not self.access_key or not self.secret_key:
            raise ValueError("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY is not set")

    def get_s3_client(self):
        logger.info(
            "[SUCCESS] S3 client called",
            extra={
                'event' : "S3"
            }
        )
        return boto3.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        
        
    def get_file(self, bucket:str, key:str):
        s3 = self.get_s3_client()
        
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj['Body'].read()
            
            suffix = Path(key).suffix.lower()
            
            # in case of xlsx
            if suffix == '.xlsx':
                df = pd.read_excel(io.BytesIO(body))
                logger.info(
                    "[SUCCESS] Read xlsx from S3: %s/%s",
                    bucket,
                    key,
                    extra={
                        "event": "S3_read",
                        "condition" : "Success"
                    },
                )
                return df
            
            # in case of parquet
            elif suffix == '.parquet':
                df = pd.read_parquet(io.BytesIO(body))
                logger.info(
                    "[SUCCESS] Read parquet from S3: %s/%s",
                    bucket,
                    key,
                    extra={
                        "event": "S3_read",
                        "condition" : "Success"
                    },
                )
                return df
                
            raise ValueError(f"Unsupported file format: {suffix}")
        
        except Exception as e:
            logger.exception(
                "Failed to read file from S3: %s/%s, error=%s",
                bucket,
                key,
                e,
                extra={
                    "event": "S3_read_error",
                    "condition" : "Fail"
                },
            )
            raise    
        
                
    def upload_file(self, df:pd.DataFrame, bucket:str, key:str):
        s3 = self.get_s3_client()
        
        suffix = Path(key).suffix.lower()
        
        try:            
            # case of parquet
            if suffix == ".parquet":
                logger.info("[DEBUG] parquet upload start")
                
                mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                logger.info("[DEBUG] df rows=%d cols=%d memory=%.2fMB", df.shape[0], df.shape[1], mem_mb)
                
                buffer = io.BytesIO()
                logger.info("[DEBUG] bytes buffer created")
                
                
                df.to_parquet(buffer, index=False, engine="pyarrow")
                logger.info("[DEBUG] df.to_parquet done, buffer_size=%d", buffer.getbuffer().nbytes)
                
                buffer.seek(0)
                logger.info("[DEBUG] buffer seek done")
                
                s3.upload_fileobj(buffer, bucket, key)
                logger.info("[DEBUG] upload_fileobj done")
                
                logger.info(
                    "[SUCESS] Uploaded parquet into S3: %s/%s",
                    bucket,
                    key,
                    extra={
                        "event": "S3_upload",
                        "condition" : "Success"
                    },
                )
            elif suffix == ".xlsx":
                logger.info("[DEBUG] xlsx upload start")

                mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                logger.info(
                    "[DEBUG] df rows=%d cols=%d memory=%.2fMB",
                    df.shape[0],
                    df.shape[1],
                    mem_mb
                )

                buffer = io.BytesIO()
                logger.info("[DEBUG] bytes buffer created")

                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False)

                logger.info(
                    "[DEBUG] df.to_excel done, buffer_size=%d",
                    buffer.getbuffer().nbytes
                )

                buffer.seek(0)
                logger.info("[DEBUG] buffer seek done")

                s3.upload_fileobj(buffer, bucket, key)
                logger.info("[DEBUG] upload_fileobj done")

                logger.info(
                    "[SUCCESS] Uploaded xlsx into S3: %s/%s",
                    bucket,
                    key,
                    extra={
                        "event": "S3_upload",
                        "condition": "Success"
                    },
                )
                
            else:
                raise ValueError("Unsupported file format: %s", suffix)
            
        except Exception as e:
            logger.exception(
                "Failed to upload file into S3: %s/%s, error=%s",
                bucket,
                key,
                e,
                extra={
                    "event": "S3_upload_error",
                    "condition" : "Fail"
                },
            )
            raise 
        
        
    def get_latest_parquet(self, bucket:str, prefix:str):
        s3 = self.get_s3_client()

        try:
            paginator = s3.get_paginator("list_objects_v2")
            
            parquet_keys = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj["Key"]
                    
                    if key.endswith("updated.parquet"):
                        parquet_keys.append(key)
                    elif key.endswith('review.parquet'):
                        parquet_keys.append(key)
            if not parquet_keys:
                raise ValueError(f"No Parquet file found under prefix: {prefix}")
            
            logger.info(
                "Total Count : %s\tData : [%s]",
                len(parquet_keys),
                ', '.join(parquet_keys[:3]),
            )
            
            latest_key = max(parquet_keys)
            response = s3.get_object(Bucket=bucket, Key=latest_key)
            
            logger.info(
                "[SUCCESS] Latest Path : %s",
                latest_key,
                extra={
                    'event': "S3_Read",
                    "condition" : "Success"
                }
            )
            
            return pd.read_parquet(io.BytesIO(response["Body"].read()))
            
            
            
        except Exception as e:
            logger.exception(
                "Failed to read file into S3: %s/%s, error=%s",
                bucket,
                prefix,
                e,
                extra={
                    "event": "S3_read_error",
                    "condition" : "Fail"
                },
            )
            raise 
        
