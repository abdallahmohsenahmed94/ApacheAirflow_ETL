#IMPORT LIBRARIES
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import tarfile
import csv
import zipfile


#FUNCTIONS
def download_dataset():
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
    destination = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    response = requests.get(url)
    if response.status_code == 200:
        with open (destination, "wb") as f:
            f.write(response.content)
    else:
        print("failed to download")

def untar_dataset():
    source = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    path = '/home/project/airflow/dags/python_etl/staging'
    with tarfile.open(source,"r:gz") as tar:
        tar.extractall(path=path)

def extract_data_from_csv():
    source = '/home/project/airflow/dags/python_etl/staging/vehicle-data.csv'
    destination = '/home/project/airflow/dags/python_etl/staging/csv_data.csv'
    with open (source, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        with open (destination, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            for row in reader:
                extracted_cols = [row[0], row[1], row[2], row[3]]
                writer.writerow(extracted_cols)

def extract_data_from_tsv():
    source= '/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv'
    destination = '/home/project/airflow/dags/python_etl/staging/tsv_data.csv'
    with open (source, mode='r', newline='') as infile:
        reader = csv.reader(infile, delimiter='\t')
        with open (destination, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            for row in reader:
                extracted_cols = [row[3], row[4], row[5]]
                writer.writerow(extracted_cols)

def extract_data_from_fixed_width():
    source= '/home/project/airflow/dags/python_etl/staging/payment-data.txt'
    destination = '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'
    with open (source, mode='r', newline='') as infile:
        with open (destination, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            for line in infile:
                row1=line[58:62].strip()
                row2=line[63:67].strip()
                extracted_cols=[row1, row2]
                writer.writerow(extracted_cols)

def consolidate_data():
    source1= '/home/project/airflow/dags/python_etl/staging/csv_data.csv'
    source2= '/home/project/airflow/dags/python_etl/staging/tsv_data.csv'
    source3= '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'
    destination= '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
    with open (source1,'r') as f1, open (source2,'r') as f2, open (source3,'r') as f3:
        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)
        reader3 = csv.reader(f3)
        with open(destination, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            for row1, row2, row3 in zip(reader1, reader2, reader3):
                consolidate_rows = row1 + row2 + row3
                writer.writerow(consolidate_rows)

def transform_data():
    source= '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
    destination= '/home/project/airflow/dags/python_etl/staging/transformed_data.csv'
    with open (source, mode='r', newline='') as infile:
        reader = csv.reader(infile)
        with open (destination, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            for row in reader:
                row[1]=row[1].upper()
                writer.writerow(row)
                

#DAG ARG
default_args={
    'owner': 'Abdallah',
    'start_date': days_ago(0),
    'email': 'abdallah.mohsenahmed94@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#DAG DEFINITION
dag=DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

#TASK DEFINITION
download_dataset_task=PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

untar_dataset_task=PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag
)

extract_data_from_csv_task=PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_data_from_tsv_task=PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag
)

extract_data_from_fixed_width_task=PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data_task=PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_data_task=PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

#PIPELINE DEFINITIONS
download_dataset_task >> untar_dataset_task >> [ extract_data_from_csv_task, extract_data_from_tsv_task, extract_data_from_fixed_width_task] >> consolidate_data_task >> transform_data_task

