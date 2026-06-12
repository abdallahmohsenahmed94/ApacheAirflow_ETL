#IMPORT LIBRARIES
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


#DAG ARGUMENTS
default_args={
    'owner': 'abdallah',
    'start_date': days_ago(0),
    'email': 'abdallah.mohsenahmed94@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


#DAG DEFINITION
dag=DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#TASKS DEFINITION
unzip_data= BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/labs',
    dag=dag,
)

extract_data_from_csv= BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/labs/vehicle-data.csv > /home/project/airflow/dags/finalassignment/labs/csv_data.csv",
    dag=dag,
)

extract_data_from_tsv= BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -d$'\t' -f5,6,7 /home/project/airflow/dags/finalassignment/labs/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/labs/tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width= BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=(
        "cut -c59-62,63-67 /home/project/airflow/dags/finalassignment/labs/payment-data.txt "
        "| tr ' ' ',' > /home/project/airflow/dags/finalassignment/labs/fixed_width_data.csv"
    ),
    dag=dag,
)

consolidate_data=BashOperator(
    task_id='consolidate_data',
    bash_command=(
        "paste -d',' "
        "/home/project/airflow/dags/finalassignment/labs/csv_data.csv "
        "/home/project/airflow/dags/finalassignment/labs/tsv_data.csv "
        "/home/project/airflow/dags/finalassignment/labs/fixed_width_data.csv "
        "> /home/project/airflow/dags/finalassignment/labs/extracted_data.csv"
    ),
    dag=dag,
)

transform_data=BashOperator(
    task_id='transform_data',
    bash_command=(
        "awk -F',' 'BEGIN {OFS=\",\"} "
        "{$4 = toupper($4); print}' "
        "/home/project/airflow/dags/finalassignment/labs/extracted_data.csv "
        "> /home/project/airflow/dags/finalassignment/labs/transformed_data.csv"
    ),
    dag=dag,
)

#TASK PIPELINE
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data

