from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from datetime import datetime
import logging


class CreateTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,sql,table,cur,*args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.cur = cur

    def execute(self, context):
        """This function has the definition necessary for creating the fact and dimension tables"""
        start = datetime.now()
        logging.info(f"Creating table {self.table}")
        print(self.sql)
        self.cur.execute(self.sql)
        logging.info(f"Created table {self.table}")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")
