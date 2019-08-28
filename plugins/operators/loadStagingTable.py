from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from datetime import datetime
import logging

class LoadStagingTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self,path,engine,table,*args, **kwargs):
        super(LoadStagingTableOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.engine = engine
        self.table = table

    def execute(self, context):
        start = datetime.now()
        print(datetime.now())
        logging.info("Loading Dataframe")

        df = pd.read_csv(self.path,sep=",",dtype='unicode')

        logging.info("dataframe Loaded")

        print(datetime.now())
        logging.info(f"Loading data into {self.table}")

        db_tab_cols = pd.read_sql(f"select * from {self.table} where 1=2", self.engine).columns.tolist()
        df.columns=db_tab_cols
        df.to_sql(self.table,self.engine,if_exists="append",chunksize=10000,index=False)

        end = datetime.now()
        time_taken = (end-start)
        logging.info(f"Data Loaded into {self.table}")
        logging.info(f"Total time taken:{time_taken}")
