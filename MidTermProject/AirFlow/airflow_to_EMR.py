import json
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

default_args={
    'email': ['gordeninottawa@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': days_ago(1),
    'depends_on_past': False,   
    'schedule_interval': None,
    'provide_context': True,   
    'catchup': False,
    'backfill': False
}

CLUSTER_ID = "j-7FZDXKM2W1L0" # EMR cluster ID

with DAG(dag_id = 'airflow_to_EMR', default_args = default_args) as dag:

    dummy_task = DummyOperator(task_id='start')

    def parse_parameter(**kwargs):
        print(type(kwargs['dag_run'].conf['file_dict']))
        print(kwargs['dag_run'].conf['file_dict'])
        for csv_file_name, s3_location in kwargs['dag_run'].conf['file_dict'].items():
            kwargs['ti'].xcom_push(key = csv_file_name, value = s3_location)

    parse_request = PythonOperator(task_id = 'parse_parameter', python_callable = parse_parameter)    

    SPARK_STEPS = [
        {
            'Name': 'Hui_mid_term',
            'ActionOnFailure': "CONTINUE",
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
                    '--class', 'Driver.MainApp',
                    '--master', 'yarn',
                    '--deploy-mode', 'cluster',
                    '--num-executors', '2',
                    '--driver-memory', '512m',
                    '--executor-memory', '3g',
                    '--executor-cores', '2',            
                    's3://hui-mid-term/midtermSpark.py', ## the S3 folder store the pyspark script.
                    '--spark_name=mid_term',
                    '--calenar_csv_location', "{{ task_instance.xcom_pull('parse_parameter', key='calendar.csv') }}",
                    '--inventory_csv_location', "{{ task_instance.xcom_pull('parse_parameter', key='inventory.csv') }}",
                    '--product_csv_location', "{{ task_instance.xcom_pull('parse_parameter', key='product.csv') }}",
                    '--sales_csv_location', "{{ task_instance.xcom_pull('parse_parameter', key='sales.csv') }}",
                    '--store_csv_location', "{{ task_instance.xcom_pull('parse_parameter', key='store.csv') }}"
                ]
            }
        }
    ]
    
    emrAddStepTask = EmrAddStepsOperator(
        task_id = 'add_emr_steps',
        job_flow_id = CLUSTER_ID,
        aws_conn_id = "midterm_emr_connection",  # connection id of AWS saved in the Airlow Adim -> Connections
        steps = SPARK_STEPS        ## SPARK_STEPS we defined in previous code
    )

    emrSensorTask = EmrStepSensor(
        task_id = 'run_emr_steps',
        job_flow_id = CLUSTER_ID,
        step_id = "{{ task_instance.xcom_pull('add_emr_steps', key='return_value')[0] }}",
        aws_conn_id = "midterm_emr_connection"
    )                

    dummy_task >> parse_request >> emrAddStepTask >> emrSensorTask

# command to run the script in local:
# /usr/bin/spark-submit --master local[*] --deploy-mode client s3://hui-mid-term/midtermSpark.py --spark_name 'midtermProject' --calenar_csv_location input/2022-12-19/calendar.csv --inventory_csv_location input/2022-12-19/inventory.csv --product_csv_location input/2022-12-19/product.csv  --sales_csv_location input/2022-12-19/sales.csv --store_csv_location input/2022-12-19/store.csv
# /usr/bin/spark-submit --master yarn --deploy-mode cluster s3://hui-mid-term/midtermSpark.py --spark_name 'midtermProject' --calenar_csv_location input/2022-12-19/calendar.csv --inventory_csv_location input/2022-12-19/inventory.csv --product_csv_location input/2022-12-19/product.csv  --sales_csv_location input/2022-12-19/sales.csv --store_csv_location input/2022-12-19/store.csv
# /usr/bin/spark-submit --class Driver.MainApp --master yarn --deploy-mode cluster --num-executors 2 --driver-memory 512m --executor-memory 3g --executor-cores 2 s3://hui-mid-term/midtermSpark.py --spark_name 'mid_term' --calenar_csv_location input/2022-12-19/calendar.csv --inventory_csv_location input/2022-12-19/inventory.csv --product_csv_location input/2022-12-19/product.csv  --sales_csv_location input/2022-12-19/sales.csv --store_csv_location input/2022-12-19/store.csv