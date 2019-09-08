#import all necessary modules
import logging
from datetime import datetime
from configparser import ConfigParser
from datetime import timedelta
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (CreateTableOperator,loadTableOperator)

from helpers import sql,etl

#Extract the configuration file
config = ConfigParser()
config.read("credentials.cfg")

DEFAULT_ARGS = {
    'owner': 'arnabguha',
    'depends_on_past': False,
    'start_date': datetime(2012, 1, 1),
    'end_date': datetime(2019, 6, 1),
    'retries': 2,
    'retries_delay': timedelta(minutes=1),
    'catchup':False
}

#define the dag
dag = DAG (
    "traffic-violations",
    default_args = DEFAULT_ARGS,
    schedule_interval = '@yearly'
)


#define the source file paths
fp_traffic = "/home/arnabguha/Projects/Datasets/mcp-traffic-violations/traffic-violations.csv"
fp_crash = '/home/arnabguha/Projects/Datasets/mcp-traffic-violations/crash-reporting-drivers-data.csv'


#Creating the engine and setting up the database connections
rdbms = config.get("DB", "rdbms")
user = config.get("DB", "user")
password = config.get("DB", "password")
host = config.get("DB", "host")
port = config.get("DB", "port")
database = config.get("DB", "database")

engine = create_engine(f'{rdbms}://{user}:{password}@{host}:{port}/{database}')
conn = ps.connect(f"host={host} dbname={database} user={user} password={password}")
cur = conn.cursor()
conn.set_session(autocommit=True)


#function definitions for different tasks
def start():
    """This is the definition of the stat task."""
    logging.info("Execution Started")

def end():
    """This is te definition of the end task"""
    logging.info("Execution Ended")

def load_dim_date(cur,table):
    """This is a function to load the dim_date dimension table"""
    logging.info(f"loading {table} table")
    cur.execute(etl.insert_dim_date)
    logging.info(f"data loaded in table {table}")

def load_fact_traffic_violations_count_agg(cur,code):
    """This is a function to load the load_fact_traffic_violations_count_agg fact table"""
    cur.execute(code)

def null_check(cur,code,table,passvalue):
    """This is a data quality check function for checking if there are null values in a table"""
    cur.execute(code)
    result = cur.fetchall()[0][0]
    if result == passvalue:
        print(f"Data Quality check passed. {table} doesn't have any null values.")
    else:
        print(f"Data quality check failed. {table} has null values.")

def hasrow_check(cur,code,table,failvalue):
    """This is a data quality check function for checking if we have rows in the tables"""
    cur.execute(code)
    result = cur.fetchall()[0][0]
    if result == failvalue:
        print(f"Data quality check failed. {table} has no rows.")
    else:
        print(f"Data quality check passed. {table} has rows")

#Start task definition
start = PythonOperator(
    task_id = "start",
    dag = dag,
    python_callable = start
)

#Definition of tasksnecessary for creating the dimension and fact tables

create_dim_date = CreateTableOperator(
    task_id = "create_dim_date",
    dag = dag,
    sql = sql.create_dim_date,
    table = "v_dim_date",
    cur=cur
)

create_dim_vehicletype = CreateTableOperator(
    task_id = "create_dim_vehicletype",
    dag = dag,
    sql = sql.create_dim_vehicle_type,
    table = "dim_vehicletype",
    cur = cur
)

create_dim_driver= CreateTableOperator(
    task_id = "create_dim_driver",
    dag = dag,
    sql = sql.create_dim_driver,
    table = "dim_driver",
    cur = cur
)

create_dim_arrest_type= CreateTableOperator(
    task_id = "create_dim_arrest_type",
    dag = dag,
    sql = sql.create_dim_arrest_type,
    table = "dim_arrest_type",
    cur = cur
)

create_dim_violation_type= CreateTableOperator(
    task_id = "create_dim_violation_type",
    dag = dag,
    sql = sql.create_dim_violation_type,
    table = "dim_violation_type",
    cur = cur
)

create_dim_subagency= CreateTableOperator(
    task_id = "create_dim_subagency",
    dag = dag,
    sql = sql.create_dim_subagency,
    table = "dim_subagency",
    cur = cur
)

create_fact_traffic_violations = CreateTableOperator(
    task_id = "create_fact_traffic_violations",
    dag = dag,
    sql = sql.create_fact_traffic_violations,
    table = "fact_traffic_violations",
    cur = cur
)

create_fact_traffic_violations_count_agg = CreateTableOperator(
    task_id = "create_fact_traffic_violations_count_agg",
    dag = dag,
    sql = sql.create_fact_traffic_violations_count_agg,
    table = "fact_traffic_violations_count_agg",
    cur = cur
)

create_dim_agency = CreateTableOperator(
    task_id = "create_dim_agency",
    dag = dag,
    sql = sql.create_dim_agency,
    table = "dim_agency",
    cur = cur
)

create_dim_acrs_type = CreateTableOperator(
    task_id = "create_dim_acrs_type",
    dag = dag,
    sql = sql.create_dim_acrs_type,
    table = "dim_acrs_type",
    cur = cur
)

create_dim_route_type = CreateTableOperator(
    task_id = "create_dim_route_type",
    dag = dag,
    sql = sql.create_dim_route_type,
    table = "dim_route_type",
    cur = cur
)

create_dim_municipality = CreateTableOperator(
    task_id = "create_dim_municipality",
    dag = dag,
    sql = sql.create_dim_municipality,
    table = "dim_municipality",
    cur = cur
)

create_dim_crashed_vehicle = CreateTableOperator(
    task_id = "create_dim_crashed_vehicle",
    dag = dag,
    sql = sql.create_dim_crashed_vehicle,
    table = "dim_crashed_vehicle",
    cur = cur
)

create_dim_cross_street_type = CreateTableOperator(
    task_id = "create_dim_cross_street_type",
    dag = dag,
    sql = sql.create_dim_cross_street_type,
    table = "dim_cross_street_type",
    cur = cur
)

create_fact_crash_details = CreateTableOperator(
    task_id = "create_fact_crash_details",
    dag = dag,
    sql = sql.create_fact_crash_details,
    table = "fact_crash_details",
    cur = cur
)

load_dim_date = PythonOperator(
    task_id="load_dim_date",
    dag = dag,
    python_callable = load_dim_date,
    op_args=[cur,"dim_date"]
)

load_dim_vehicle_type = loadTableOperator(
    task_id = "load_dim_vehicle_type",
    dag=dag,
    path=fp_traffic,
    code=etl.vehicle_type,
    table="dim_vehicle_type",
    engine=engine,
    cur=cur
)

load_dim_driver = loadTableOperator(
    task_id = "load_dim_driver",
    dag=dag,
    path=fp_traffic,
    code=etl.driver,
    table="dim_driver",
    engine=engine,
    cur=cur
)

load_dim_arrest_type = loadTableOperator(
    task_id = "load_dim_arrest_type",
    dag=dag,
    path=fp_traffic,
    code=etl.arrest_type,
    table="dim_arrest_type",
    engine=engine,
    cur=cur
)

load_dim_violation_type = loadTableOperator(
    task_id = "load_dim_violation_type",
    dag=dag,
    path=fp_traffic,
    code=etl.violation_type,
    table="dim_violation_type",
    engine=engine,
    cur=cur
)

load_dim_subagency = loadTableOperator(
    task_id = "load_dim_subagency",
    dag=dag,
    path=fp_traffic,
    code=etl.subagency,
    table="dim_subagency",
    engine=engine,
    cur=cur
)

load_fact_traffic_violations = loadTableOperator (
    task_id = "load_fact_traffic_violations",
    dag = dag,
    path=fp_traffic,
    code=etl.fact_traffic_violations,
    table="fact_traffic_violations",
    engine=engine,
    cur=cur
)

load_fact_traffic_violations_count_agg = PythonOperator (
    task_id = "load_fact_traffic_violations_count_agg",
    dag = dag,
    python_callable=load_fact_traffic_violations_count_agg,
    op_args=[cur,etl.insert_fact_traffic_violations_count_agg]
)

load_dim_agency = loadTableOperator(
    task_id = "load_dim_agency",
    dag=dag,
    path=fp_crash,
    code=etl.load_dim_agency,
    table="dim_agency",
    engine=engine,
    cur=cur
)

load_dim_acrs = loadTableOperator(
    task_id = "load_dim_acrs",
    dag=dag,
    path=fp_crash,
    code=etl.load_dim_acrs,
    table="dim_acrs_type",
    engine=engine,
    cur=cur
)

load_dim_route_type = loadTableOperator(
    task_id = "load_dim_route_type",
    dag=dag,
    path=fp_crash,
    code=etl.load_dim_route_type,
    table="dim_route_type",
    engine=engine,
    cur=cur
)

load_dim_municipality = loadTableOperator(
    task_id = "load_dim_municipality",
    dag=dag,
    path=fp_crash,
    code=etl.load_dim_municipality,
    table="dim_municipality",
    engine=engine,
    cur=cur
)

load_dim_crashed_vehicle = loadTableOperator(
    task_id = "load_dim_crashed_vehicle",
    dag=dag,
    path=fp_crash,
    code=etl.load_dim_crashed_vehicle,
    table="dim_crashed_vehicle",
    engine=engine,
    cur=cur
)

load_dim_cross_street_type = loadTableOperator(
    task_id = "load_dim_cross_street_type",
    dag=dag,
    path=fp_crash,
    code=etl.load_dim_cross_street_type,
    table="dim_cross_street_type",
    engine=engine,
    cur=cur
)

load_fact_crash_details = loadTableOperator (
    task_id = "load_fact_crash_details",
    dag = dag,
    path=fp_crash,
    code=etl.load_fact_crash_details,
    table="fact_crash_details",
    engine=engine,
    cur=cur
)

#Task definitions necessary for data quality checks

fact_traffic_violations_null_check = PythonOperator(
    task_id = "fact_traffic_violations_null_check",
    dag=dag,
    python_callable=null_check,
    op_args=[cur,sql.fact_traffic_violations_null_check,"fact_traffic_violations",0]
)

fact_traffic_violations_count_agg_null_check = PythonOperator(
    task_id = "fact_traffic_violations_count_agg_null_check",
    dag=dag,
    python_callable=null_check,
    op_args=[cur,sql.fact_traffic_violations_count_agg_null_check,"fact_traffic_violations_count_agg",0]
)

fact_crash_details_null_check = PythonOperator(
    task_id = "fact_crash_details_null_check",
    dag=dag,
    python_callable=null_check,
    op_args=[cur,sql.fact_crash_details_null_check,"fact_crash_details",0]
)

fact_traffic_violations_has_rows = PythonOperator(
    task_id = "fact_traffic_violations_has_rows",
    dag = dag,
    python_callable = hasrow_check,
    op_args=[cur,sql.fact_traffic_violations_has_rows,"fact_traffic_violations",0]
)

fact_traffic_violations_count_agg_has_rows = PythonOperator(
    task_id = "fact_traffic_violations_count_agg_has_rows",
    dag = dag,
    python_callable = hasrow_check,
    op_args=[cur,sql.fact_traffic_violations_count_agg_has_rows,"fact_traffic_violations_count_agg",0]
)

fact_crash_details_has_rows = PythonOperator(
    task_id = "fact_crash_details_has_rows",
    dag = dag,
    python_callable = hasrow_check,
    op_args=[cur,sql.fact_crash_details_has_rows,"fact_crash_details",0]
)

#End task definition
end = PythonOperator(
    task_id = "end",
    dag = dag,
    python_callable = end
)


#Definition of the tasks flow
start >> create_dim_date
start >> create_dim_vehicletype
start >> create_dim_driver
start >> create_dim_arrest_type
start >> create_dim_violation_type
start >> create_dim_subagency

create_dim_date >> load_dim_date
create_dim_vehicletype >> load_dim_vehicle_type
create_dim_driver >> load_dim_driver
create_dim_arrest_type >> load_dim_arrest_type
create_dim_violation_type >> load_dim_violation_type
create_dim_subagency >> load_dim_subagency

load_dim_date >> create_fact_traffic_violations
load_dim_vehicle_type >> create_fact_traffic_violations
load_dim_driver >> create_fact_traffic_violations
load_dim_arrest_type >> create_fact_traffic_violations
load_dim_violation_type >> create_fact_traffic_violations
load_dim_subagency >> create_fact_traffic_violations


load_dim_date >> create_fact_traffic_violations_count_agg
load_dim_subagency >> create_fact_traffic_violations_count_agg

create_fact_traffic_violations >> load_fact_traffic_violations
create_fact_traffic_violations_count_agg >> load_fact_traffic_violations_count_agg
load_fact_traffic_violations >>load_fact_traffic_violations_count_agg


start >> create_dim_agency
start >> create_dim_acrs_type
start >> create_dim_route_type
start >> create_dim_municipality
start >> create_dim_crashed_vehicle
start >> create_dim_cross_street_type

create_dim_agency >> load_dim_agency
create_dim_acrs_type >> load_dim_acrs
create_dim_route_type >> load_dim_route_type
create_dim_municipality >> load_dim_municipality
create_dim_crashed_vehicle >> load_dim_crashed_vehicle
create_dim_cross_street_type >> load_dim_cross_street_type

load_dim_agency >> create_fact_crash_details
load_dim_acrs >> create_fact_crash_details
load_dim_route_type >> create_fact_crash_details
load_dim_municipality >> create_fact_crash_details
load_dim_crashed_vehicle >> create_fact_crash_details
load_dim_cross_street_type >> create_fact_crash_details
load_dim_date >> create_fact_crash_details

create_fact_crash_details >> load_fact_crash_details

load_fact_traffic_violations >> fact_traffic_violations_has_rows
load_fact_traffic_violations >> fact_traffic_violations_null_check

load_fact_traffic_violations_count_agg >> fact_traffic_violations_count_agg_has_rows
load_fact_traffic_violations_count_agg >> fact_traffic_violations_count_agg_null_check

load_fact_crash_details >> fact_crash_details_has_rows
load_fact_crash_details >> fact_crash_details_null_check


fact_traffic_violations_has_rows >> end
fact_traffic_violations_null_check >> end
fact_traffic_violations_count_agg_has_rows >> end
fact_traffic_violations_count_agg_null_check >> end
fact_crash_details_has_rows >> end
fact_crash_details_null_check >> end
