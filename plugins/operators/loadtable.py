from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from datetime import datetime
import logging

class LoadTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,sql,table,cur,*args, **kwargs):
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.cur = cur

    def execute(self, context):
        start = datetime.now()
        logging.info(f"Loading table {self.table}")
        self.cur.execute(self.sql)
        logging.info(f"Loaded table {self.table}")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")
