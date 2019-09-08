#import necessayr modules
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from datetime import datetime
import logging


class loadTableOperator(BaseOperator):
    """This class initiate the function and sources all necessary arguments"""
    @apply_defaults
    def __init__(self,path,code,table,engine,cur,*args, **kwargs):
        super(loadTableOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.code = code
        self.table = table
        self.engine = engine
        self.cur = cur

    def execute(self, context):
        """This function has the definition necessary for loading fact and dimension tables"""
        df = pd.read_csv(self.path,sep=',',dtype='unicode')
        start = datetime.now()
        logging.info(f"Loading table {self.table}")
        exec(self.code)
        logging.info(f"Loaded table {self.table}")
        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Time taken:{time_taken}")
