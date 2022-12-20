import boto3
import json
import requests
from datetime import datetime, timedelta
from send_email import send_email

class Validator:
    
    def __init__(self) -> None:
        datestr = (datetime.now() - timedelta(hours=5)).strftime("%Y-%m-%d")
        self.today_required_file_list = [f'calendar.csv', f'inventory.csv', f'product.csv',f'sales.csv', f'store.csv']
        
        self.file_dict = {}
        s3_client=boto3.client('s3')
        for object in s3_client.list_objects_v2(Bucket='hui-mid-term')['Contents']:
            if datestr not in object["Key"]:
                print(f"{datestr} not in {object['Key']}, skip")
                continue
            self.file_dict[object["Key"].split("/")[-1]] = object["Key"]
            # {
            #   "calendar.csv": "input/2022-12-15/calendar.csv",
            #   "inventory.csv": "input/2022-12-15/inventory.csv",
            #   "product.csv": "input/2022-12-15/product.csv",
            #   "sales.csv": "input/2022-12-15/sales.csv",
            #   "store.csv": "input/2022-12-15/store.csv"
            # }
        
    def checkFiles(self):
        for each_today_required_file in self.today_required_file_list:
            if each_today_required_file not in self.file_dict:
                print(f"{each_today_required_file} not existed!")
                return False
        return True

def lambda_handler(event, context):
    validator = Validator()

    # scan S3 bucket
    if validator.checkFiles():
        EC2_url = "ec2-54-226-180-66.compute-1.amazonaws.com:8080"
        dag_id = "airflow_to_EMR"
        data = {
            "conf": {
                "file_dict": validator.file_dict
            }
        }
        # send signal to Airflow    
        endpoint= f'http://{EC2_url}/api/v1/dags/{dag_id}/dagRuns'
        header = {
            "Content-Type": "application/json"
        }
        response = requests.post(endpoint, data=json.dumps(data), auth=("airflow", "airflow"), headers=header, timeout=10)
        print(f'API call to Airflow, response code {response.status_code}, response content {response.text}')
    else:
        send_email()