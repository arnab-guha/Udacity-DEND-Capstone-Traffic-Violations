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
from airflow.operators import (CreateTableOperator,LoadTableOperator)
#from airflow.operators import CreateTableOperator

from helpers import capstone_sql_queries as sql


DEFAULT_ARGS = {
    'owner': 'arnabguha',
    'depends_on_past': False,
    #'start_date': datetime(2012, 1, 1),
    #'end_date': datetime(2018, 12, 1),
    'retries': 1,
    'retries_delay': timedelta(minutes=1),
    'catchup':False
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

#Define the tasks
start_operator = PythonOperator(
    task_id='begin_execution',
    dag=dag,
    python_callable=begin
)

#Create the staging table for traffic violations
create_stg_traffic_violaitions = CreateTableOperator(
    task_id = "create_stg_traffic_violaitions",
    dag = dag,
    sql = sql.create_stg_traffic_violaitions,
    table = "stg_traffic_violations",
    cur = cur
)

create_dim_state= CreateTableOperator(
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

load_dim_state = LoadStagingTableOperator (
    task_id = "load_dim_state",
    dag = dag,
    path = fp_statecode,
    engine = engine,
    table = "dim_state"
)


#create the dim_date table
create_dim_date= CreateTableOperator(
    task_id = "create_dim_date",
    dag = dag,
    sql = sql.create_dim_date,
    table = "dim_date",
    cur = cur
)

#create the dim_vehicle_type table
create_dim_vehicletype = CreateTableOperator(
    task_id = "create_dim_vehicletype",
    dag = dag,
    sql = sql.create_dim_vehicletype,
    table = "dim_vehicletype",
    cur = cur
)
#create the dim_race table
create_dim_race= CreateTableOperator(
    task_id = "create_dim_race",
    dag = dag,
    sql = sql.create_dim_race,
    table = "dim_race",
    cur = cur
)
#create the dim_arrest_type table
create_dim_arrest_type= CreateTableOperator(
    task_id = "create_dim_arrest_type",
    dag = dag,
    sql = sql.create_dim_arrest_type,
    table = "dim_arrest_type",
    cur = cur
)
#create the dim_violation_type table
create_dim_violation_type= CreateTableOperator(
    task_id = "create_dim_violation_type",
    dag = dag,
    sql = sql.create_dim_violation_type,
    table = "dim_violation_type",
    cur = cur
)
#create the dim_subagency table
create_dim_subagency= CreateTableOperator(
    task_id = "create_dim_subagency",
    dag = dag,
    sql = sql.create_dim_subagency,
    table = "dim_subagency",
    cur = cur
)
#create the dim_driver table
create_dim_driver= CreateTableOperator(
    task_id = "create_dim_driver",
    dag = dag,
    sql = sql.create_dim_driver,
    table = "dim_driver",
    cur = cur
)


#insert into dim_date
insert_dim_date = LoadTableOperator (
    task_id = "insert_dim_date",
    dag = dag,
    sql = sql.insert_dim_date,
    table = "dim_date",
    cur = cur
)

#insert into dim_vehicle_type
insert_dim_vehicletype = LoadTableOperator (
    task_id = "insert_dim_vehicletype",
    dag = dag,
    sql = sql.insert_dim_vehicletype,
    table = "dim_vehicletype",
    cur = cur
)

#insert into dim_race
insert_dim_race = LoadTableOperator (
    task_id = "insert_dim_race",
    dag = dag,
    sql = sql.insert_dim_race,
    table = "dim_race",
    cur = cur
)

#insert into dim_driver
insert_dim_driver = LoadTableOperator (
    task_id = "insert_dim_driver",
    dag = dag,
    sql = sql.insert_dim_driver,
    table = "dim_driver",
    cur = cur
)


#insert into dim_arrest_type
insert_dim_arrest_type = LoadTableOperator (
    task_id = "insert_dim_arrest_type",
    dag = dag,
    sql = sql.insert_dim_arrest_type,
    table = "dim_arrest_type",
    cur = cur
)

#insert into dim_violation_type
insert_dim_violation_type = LoadTableOperator (
    task_id = "insert_dim_violation_type",
    dag = dag,
    sql = sql.insert_dim_violation_type,
    table = "dim_violation_type",
    cur = cur
)

#insert into dim_subagency
insert_dim_subagency = LoadTableOperator (
    task_id = "insert_dim_subagency",
    dag = dag,
    sql = sql.insert_dim_subagency,
    table = "dim_subagency",
    cur = cur
)


#create fact_traffic_violations table
create_fact_traffic_violations= CreateTableOperator(
    task_id = "create_fact_traffic_violations",
    dag = dag,
    sql = sql.create_fact_traffic_violations,
    table = "fact_traffic_violations",
    cur = cur
)
#create fact_traffic_violations_count_agg table
create_fact_traffic_violations_count_agg = CreateTableOperator(
    task_id = "create_fact_traffic_violations_count_agg",
    dag = dag,
    sql = sql.create_fact_traffic_violations_count_agg,
    table = "fact_traffic_violations_count_agg",
    cur = cur
)

#load fact_traffic_violations table
insert_fact_traffic_violations = LoadTableOperator (
    task_id = "insert_fact_traffic_violations",
    dag = dag,
    sql = sql.insert_fact_traffic_violations,
    table = "fact_traffic_violations",
    cur = cur
)
#load fact_traffic_violations_count_agg table
insert_fact_traffic_violations_count_agg = LoadTableOperator (
    task_id = "insert_fact_traffic_violations_count_agg",
    dag = dag,
    sql = sql.insert_fact_traffic_violations_count_agg,
    table = "fact_traffic_violations_count_agg",
    cur = cur
)

create_index_stg_traffic_violations = CreateIndexOperator (
    task_id = "create_index_stg_traffic_violations",
    dag = dag,
    sql = sql.create_index_stg_traffic_violations,
    cur = cur
)


end_operator = PythonOperator(
    task_id='end_execution',
    dag=dag,
    python_callable=begin
)


start_operator >> create_stg_traffic_violaitions
start_operator >> create_dim_date
start_operator >> create_dim_race
start_operator >> create_dim_state
start_operator >> create_dim_subagency
start_operator >> create_dim_arrest_type
start_operator >> create_dim_vehicletype
start_operator >> create_dim_violation_type
create_dim_race >> create_dim_driver
create_dim_state >> create_dim_driver

create_dim_date >> load_staging_traffic
create_dim_race >> load_staging_traffic
create_dim_state >> load_staging_traffic
create_dim_subagency >> load_staging_traffic
create_dim_arrest_type >> load_staging_traffic
create_dim_vehicletype >> load_staging_traffic
create_dim_violation_type >> load_staging_traffic
create_dim_driver >> load_staging_traffic
create_stg_traffic_violaitions >> load_staging_traffic

load_staging_traffic >> create_index_stg_traffic_violations

create_index_stg_traffic_violations >> insert_dim_date
create_index_stg_traffic_violations >> insert_dim_race
create_index_stg_traffic_violations >> insert_dim_subagency
create_index_stg_traffic_violations >> insert_dim_arrest_type
create_index_stg_traffic_violations >> insert_dim_vehicletype
create_index_stg_traffic_violations >> insert_dim_violation_type
create_index_stg_traffic_violations >> load_dim_state
insert_dim_race >> insert_dim_driver
load_dim_state >> insert_dim_driver

insert_dim_date >> create_fact_traffic_violations
insert_dim_subagency >> create_fact_traffic_violations
insert_dim_arrest_type >> create_fact_traffic_violations
insert_dim_vehicletype >> create_fact_traffic_violations
insert_dim_violation_type >> create_fact_traffic_violations
insert_dim_driver >> create_fact_traffic_violations

insert_dim_date >> create_fact_traffic_violations_count_agg
insert_dim_subagency >> create_fact_traffic_violations_count_agg

create_fact_traffic_violations >> insert_fact_traffic_violations
create_fact_traffic_violations_count_agg >> insert_fact_traffic_violations_count_agg

insert_fact_traffic_violations >> end_operator
insert_fact_traffic_violations_count_agg >> end_operator
