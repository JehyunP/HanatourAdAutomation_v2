# Hook
from airflow.hooks.base_hook import BaseHook

from configs.sftp import *

import io
import paramiko
import logging

# setup logging format
logger = logging.getLogger(__name__)

class SFTPHook(BaseHook):
    """
        Connect to advertisement server via SFTP
        upload the EP dataset into server as tsv (given format)
    """
    
    def __init__(
        self
    ):
        super().__init__()
        
        self.host = SFTP_HOST
        self.user = SFTP_USER
        self.password = SFTP_PASSWORD
        self.port = int(SFTP_PORT)
        
        self.conn = None
        self.sftp = None


    def get_connection(self):
        try:
            self.conn = paramiko.SSHClient()
            self.conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            self.conn.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                timeout=10
            )

            self.sftp = self.conn.open_sftp()

            logger.info(
                "[SUCCESS] Connected to Server via SFTP",
                extra={
                    'event': "SFTP",
                    'condition': "Success"
                }
            )

            return self.sftp

        except paramiko.AuthenticationException:
            logger.error(
                "[ERROR] SFTP Auth Failed - Wrong username | password",
                extra={
                    'event': "SFTP",
                    'condition': "Fail"
                }
            )
            raise

        except paramiko.SSHException as e:
            logger.error(
                "[ERROR] SFTP SSH Failed | %s",
                e,
                extra={
                    'event': "SFTP",
                    'condition': "Fail"
                }
            )
            raise

        except Exception as e:
            logger.error(
                "[ERROR] SFTP Connected Failed | %s",
                e,
                extra={
                    'event': "SFTP",
                    'condition': "Fail"
                }
            )
            raise
        
        
    def upload_df(
        self,
        df,
        path,
        encoding = "utf-8-sig"
    ):
        try:
            txt_buffer = io.StringIO()
            df.to_csv(txt_buffer, index=False, sep="\t", lineterminator='\r\n')
            
            data = txt_buffer.getvalue().encode(encoding=encoding)
            bio = io.BytesIO(data)
            bio.seek(0)
            
            sftp = self.get_connection()
            
            try:
                sftp.putfo(bio, path)
            finally:
                self.close()
                
            logger.info(
                "[SUCCESS] Upload to SFTP : %s",
                path,
                extra={
                    'event' : "SFTP upload",
                    'condition' : "Success"
                }
            )
            
        except Exception as e:
            logger.error(
                "[ERROR] SFTP upload failed: %s\n\t%s",
                path,
                e,
                extra={
                    'event' : "SFTP upload",
                    'condition' : "Fail"
                }
            )
            raise
        
    def close(self):
        try:
            if self.sftp:
                self.sftp.close()
                self.sftp = None

            if self.conn:
                self.conn.close()
                self.conn = None

            logger.info(
                "[SUCCESS] SFTP Connection Closed",
                extra={
                    'event': "SFTP",
                    'condition': "Success"
                }
            )

        except Exception as e:
            logger.error(
                "[ERROR] Failed to close SFTP connection | %s",
                e,
                extra={
                    'event': "SFTP",
                    'condition': "Fail"
                }
            )
        