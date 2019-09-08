#from airflow.plugins_manager import AirflowPlugin
#import operators

#class MyAirflowPlugin(AirflowPlugin):
#    name="My_Airflow_Plugin"
#    operators=[
#    operators.PostgresRowCountOperator,
#    operators.MyFirstOperator
#    ]

from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.loadTableOperator,
        operators.CreateTableOperator
    ]
    helpers = [
        helpers.sql,
        helpers.etl
    ]
