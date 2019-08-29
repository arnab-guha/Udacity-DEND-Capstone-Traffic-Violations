from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import logging

class CreateIndexOperator(BaseOperator):

    @apply_defaults
    def __init__(self,sql,cur,*args, **kwargs):
        super(CreateIndexOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.cur = cur

    def execute(self, context):
        start = datetime.now()

        logging.info(f"Creating index")
        self.cur.execute(self.sql)
        
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")
