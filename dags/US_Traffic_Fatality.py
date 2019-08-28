#import necessary modules
from datetime import datetime
import logging
import pandas as pd
from datetime import timedelta
from configparser import ConfigParser

from sqlalchemy import create_engine
import psycopg2 as ps

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (LoadStagingTableOperator,CreateTableOperator)
#from airflow.operators import CreateTableOperator

from helpers import sql_queries as sql


DEFAULT_ARGS = {
    'owner': 'arnabguha',
    'depends_on_past': False,
    #'start_date': datetime(2012, 1, 1),
    #'end_date': datetime(2018, 12, 1),
    'retries': 1,
    'retries_delay': timedelta(minutes=1),
    'catchup': True
}

#define the dag
dag = DAG (
    "US-Traffic_Fatality",
    default_args = DEFAULT_ARGS,
    start_date = datetime.now()
    #schedule_interval = '@yearly'
)


#define the source file paths
fp_traffic = "/home/arnabguha/Projects/Datasets/usa-traffic-violations/traffic-violations.csv"
fp_statecode = "/home/arnabguha/Projects/Datasets/north_america_state_codes.csv"


#Creating the engine and setting up the database connections
engine = create_engine('postgresql://postgres:password@localhost:5432/postgres')
conn = ps.connect("host=localhost dbname=postgres user=postgres password=password")
cur = conn.cursor()
conn.set_session(autocommit=True)


#load the source files into Dataframes
#df_traffic = pd.read_csv(fp_traffic,sep=",",dtype='unicode')
#df_state = pd.read_csv(fp_statecode,sep=",",dtype='unicode')

#Define Begin and End Execution
def begin():
    logging.info("Execution began")

def end():
    logging.info("Execution ended")

start_operator = PythonOperator(
    task_id='begin_execution',
    dag=dag,
    python_callable=begin
)

create_table_staging_traffic = CreateTableOperator(
    task_id = "create_stg_traffic_violaitions",
    dag = dag,
    sql = sql.create_stg_traffic_violaitions,
    table = "stg_traffic_violations",
    cur = cur
)

create_table_staging_state= CreateTableOperator(
    task_id = "create_dim_state",
    dag = dag,
    sql = sql.create_dim_state,
    table = "dim_state",
    cur = cur
)

load_staging_traffic = LoadStagingTableOperator (
    task_id = "load_staging_traffic",
    dag = dag,
    path = fp_traffic,
    engine = engine,
    table = "stg_traffic_violations"
)

load_staging_state = LoadStagingTableOperator (
    task_id = "load_staging_state",
    dag = dag,
    path = fp_statecode,
    engine = engine,
    table = "dim_state"
)

end_operator = PythonOperator(
    task_id='end_execution',
    dag=dag,
    python_callable=begin
)

start_operator >> create_table_staging_traffic
start_operator >> create_table_staging_state

create_table_staging_traffic >> load_staging_traffic
create_table_staging_state >> load_staging_state

load_staging_traffic >> end_operator
load_staging_state >> end_operator
