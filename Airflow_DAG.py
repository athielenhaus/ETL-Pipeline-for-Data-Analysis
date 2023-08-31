''' 
DAG for Calculating Article Completeness including Score:
1. Create table with information about article duplicates for previous month
2. Create table with information about detected article categories for previous month
3. Create table with results of article completeness analysis for each visible article from previous month
4. Insert aggregated results into a table which can be used as source for Tableau Dashboard
5. Calculate article completeness scores and insert into another table which can be used for Tableau
'''


# IMPORT LIBRARIES
from airflow import DAG
 
# operators:
import pyexasol
from operators.ExasolOperator import ExasolOperator             # custom-made operator based on pyexasol
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.sensors.external_task_sensor import ExternalTaskSensor 

# utils:
from airflow.hooks.base_hook import BaseHook   
import pendulum                                
from datetime import datetime, timedelta       
import sys

# set path to import modules from other python file
path = "/dags/airflow-dags.git/analytics_dept/python/"
sys.path.insert(0, path)

# PROVIDE DEFAULT ARGUMENTS
default_args = {
    "owner": "CAMELOT_ANALYTICS",                                             
    "start_date": pendulum.datetime(2022, 9, 1, 12, 0, tz='Europe/Madrid'),   # a fix date including an interval in the past. Pendulum allows for setting a timezone
    "depends_on_past": False,                                                 # when set to True, keeps a task from getting triggered if the previous schedule for the task hasn’t succeeded.
    "email": "arne.thielenhaus@gmail.com",          
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,                                   # in case of failure, the number of retries
    "retry_delay": timedelta(minutes=5)             # time to wait for each retry
}   


# set variable for table names and for time frame
week_or_month = 'MONTH'
last_month = pendulum.now('Europe/Madrid').add(months=-1)
start_date = last_month.start_of('month').to_date_string()
end_date = last_month.end_of('month').to_date_string()


'''
Weekly alternative

# set variable for table names and for time frame
week_or_month = 'WEEK'
last_week = pendulum.now('Europe/Madrid').add(weeks=-1)
start_date = last_week.start_of('week').to_date_string()
end_date = last_week.end_of('week').to_date_string()
'''

# main path to SQL scripts folder
path = "/dags/airflow-dags.git/analytics_dept/python/ac_analysis_scripts/"
sys.path.insert(0, path)
    
    
# CREATE DAG
dag = DAG(
    dag_id="Camelot_Article_Completeness_Monthly",      # DAG name which appears on GUI
    default_args=default_args,                          # default args defined above
    schedule_interval= '0 12 1 * *',                    # runs at 12 noon every first day of the month.
    catchup=False
)


task_1 = ExasolOperator(
    task_id='Check_for_detected_article_categories.sql',
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + '00_Check_for_detected_article_categories.sql',
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
)

#To-Do: check for successful article category check

task_2 = ExasolOperator(
    task_id='Check_for_duplicates.sql',
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + '00_Check_for_duplicates_autom.sql',
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
)


#To-Do: check for succesful duplicate check

task_3 = ExasolOperator(
    task_id='AC_Analysis.sql',
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + '01_AC_Analysis_autom.sql',
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
)                             
     
                 
task_4 = ExasolOperator(
    task_id='Calculate_AC_Scores.sql',
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + '03_calculate_AC_scores.sql',
    parameters={
        'week_or_month': week_or_month
    },
    dag=dag
    )



# Run Email operator to send Email Confirmation indicating successful run:
sendEmail = EmailOperator(
        task_id='send_email',
        to='arne.thielenhaus@gmail.com',
        subject='Airflow Test Result',
        html_content=f'<h3>Airflow Task completed!',
        provide_context=True,
        dag=dag)            
    

# Establish Workflow
task_1 >> task_2 >> task_3 >> task_4 >> sendEmail 



