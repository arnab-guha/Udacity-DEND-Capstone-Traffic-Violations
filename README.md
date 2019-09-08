## DEND Capstone Project - Traffic Violations and Crash Data for Montgomery County Police Department
Capstone project for Udacity Data Engineering Nanodegree course

### Objective:
The objective of this project is to create a data model and ETL flow for Montgomery County Police department which will load Traffic violations and crash details. The data comes from th MCP's open data website (https://data.montgomerycountymd.gov/browse). The data spans across multiple years and have more than a million rows. For this project we have used Traffic violation data and Vehicle Crash details data. 

### Datasource Details:
The data is gathered from Montgomery County Police department's open data catalog. Following datasets are in scope of this project:
- traffic-violation.csv
- crash-reporting-drivers-data.csv

Here are some snippets of the datasets:

Traffic Violation
![traffic-violation](https://github.com/arnab-guha/Udacity-DEND-Capstone-Traffic-Violations/blob/master/traffic.PNG)

Crash Details
![Crash-Details](https://github.com/arnab-guha/Udacity-DEND-Capstone-Traffic-Violations/blob/master/crash.PNG)

### Project Scope:
The scope of this project is to create a data pipeline which will accept the source files, process and clean them, transform as per the the need of the final data model and load them in dimension and fact tables. We are going to read the source files from local storage, use airflow and python to create a data pipeline, and eventually load the processed and transformed data into the data model created in local postgresql database. 

### Technology used:
- Apache Airflow
- Python
- PostgreSQL

### Data Model
The final data model consists of 12 dimension and 3 fact tables. Following is the data model diagram:
![Data-Model](https://github.com/arnab-guha/Udacity-DEND-Capstone-Traffic-Violations/blob/master/data%20model.PNG)

### Data Pipeline Design
The data pipline was designed using Apache Airflow. The whole process was segregated in several phases:
- Creating the dimension tables
- Loading the dimension tables 
- Creating the facts tables
- Loading the fact tables
- Performing data quality checks

Following is the airflow dag for the whole process:

![airflow-dag](https://github.com/arnab-guha/Udacity-DEND-Capstone-Traffic-Violations/blob/master/airflow_dag.PNG)

### Addressing Other Scenarios

#### The data was increased by 100x.

As the size increase, handling data from local csv files would not be feasible. In that case moving the files to Amazon S3 and loading them using Spark would be a feasible choice. 

#### The pipelines would be run on a daily basis by 7 am every day.

This can be handled using the existing Airflow DAG using the scheduling feature of Airflow. 

#### The database needed to be accessed by 100+ people.

Local postgresql database would not be able to handle the load of 100+ people. In that case instead of using local postgresql, we would use Amazon redshift as redshift has clustering abilities and thanks to AWS, its highly scalable. 
