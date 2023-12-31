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
from airflow.sensors.external_task_sensor import ExternalTaskSensor 

# utils:
from airflow.hooks.base_hook import BaseHook   
import pendulum                                
from datetime import datetime, timedelta       
import sys

# import qa script from other file
from qa import check_table_length

# PROVIDE DEFAULT ARGUMENTS
default_args = {
    "owner": "CAMELOT_ANALYTICS",                                             
    "start_date": pendulum.datetime(2022, 9, 1, 12, 0, tz='Europe/Madrid'),   # a fix date including an interval in the past. Pendulum allows for setting a timezone
    "depends_on_past": False,                                                 # when set to True, keeps a task from getting triggered if the previous schedule for the task hasn’t succeeded.
    "email": "arne.thielenhaus@gmail.com",          
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,                                   # in case of failure, the number of retries
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

check_external_dag_completion = ExternalTaskSensor(
    task_id='check_external_dag_completion',
    external_dag_id='daily_articles_DB_update',
    external_task_id= None, 
    timeout=7200,  # Timeout in seconds (2 hours)
    poke_interval=1200,  # Time in seconds that the job should wait in between each try during the poking (20 minutes)
    mode='poke'  # Mode can be 'poke' or 'reschedule'
)


file_1a = '00_Check_for_detected_article_categories.sql'
run_cat_det_check = ExasolOperator(
    task_id= file_1a,
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + file_1a,
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
)

#check for successful article category check (row count)
cat_table= f'ANALYTICS_DB.ARTICLE_CATEGORY_DETECTED_BOOLEAN_{week_or_month}LY'
check_cat_tbl_length = PythonOperator(
    task_id='check_cat_tbl_length',
    python_callable=check_table_length,
    op_kwargs={
        'table': cat_table,
        'min_rows': 100000      # expected row count
    },
    dag=dag,
)

file_1b = '00_Check_for_duplicates_autom.sql'
run_dup_check = ExasolOperator(
    task_id= file_1b,
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + file_1b,
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
)


#Check for successful duplicate check (row count)
dup_table = f'ANALYTICS_DB.AC_DUPLICATE_CHECK_{week_or_month}LY'
check_dup_tbl_length = PythonOperator(
    task_id='check_dup_tbl_length',
    python_callable=check_table_length,
    op_kwargs={
        'table': dup_table,
        'min_rows': 100000      # expected row count
    },
    dag=dag,
)

file_2 = '01_AC_Analysis.sql'
run_ac_analysis = ExasolOperator(
    task_id= file_2,
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + file_2,
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
) 

#Check for successful AC Analysis (row count)
ac_table = f'ANALYTICS_DB.AC_ANALYSIS_{week_or_month}LY'
check_ac_tbl_length = PythonOperator(
    task_id='check_ac_tbl_length',
    python_callable=check_table_length,
    op_kwargs={
        'table': ac_table,
        'min_rows': 100000      # expected row count
    },
    dag=dag,
)

file_3 = '02_Aggregate_results_for_individual_criteria.sql'
aggregate_results = ExasolOperator(
    task_id = file_3,
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + file_3,
    parameters={
        'week_or_month': week_or_month,
        'start_date': start_date,
        'end_date': end_date
    },
    dag=dag
)         
     
file_4 = '03_calculate_AC_scores.sql'               
calc_ac_scores = ExasolOperator(
    task_id= file_4,
    jdbc_conn_id='camelot_analytics_dept',
    sqlfile=path + file_4,
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
check_external_dag_completion >> [run_cat_det_check, run_dup_check] >> [check_cat_tbl_length, check_dup_tbl_length] >> run_ac_analysis >> check_ac_tbl_length >> aggregate_results >> calc_ac_scores >> sendEmail 



