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

CLUSTER_ID = "j-2TK9VQVQUJ9KP" # EMR cluster ID

with DAG(dag_id = 'airflow_to_EMR', default_args = default_args) as dag:

    dummy_task = DummyOperator(task_id='start')

    def parse_parameter(**kwargs):
        print(kwargs['dag_run'].conf['file_dict'])
        kwargs['ti'].xcom_push(key = 'input_file_url', value = kwargs['dag_run'].conf['file_dict'])

    parse_request = PythonOperator(task_id = 'parse_parameter', python_callable = parse_parameter)    

    SPARK_STEPS = [
        {
            'Name': 'Hui_mid_term',
            'ActionOnFailure': "CONTINUE",
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    's3://hui-mid-term/midtermSpark.py', ## the S3 folder store the pyspark script.
                    '--spark_name', 'mid_term',
                    '--input_file_url', "{{ task_instance.xcom_pull('parse_parameter', key='input_file_url') }}"
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