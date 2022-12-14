import boto3
import time
import json
import subprocess
from send_email import send_email

class Validator:
    
    def __init__(self) -> None:
        datestr = time.strftime("%Y-%m-%d")
        self.today_required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv',f'sales_{datestr}.csv', f'store_{datestr}.csv', "NotExisted"]
        
        s3_file_dict = {}
        s3_client=boto3.client('s3')
        for object in s3_client.list_objects_v2(Bucket='hui-mid-term')['Contents']:
            s3_file_dict[object["Key"].split("/")[-1]] = object["Key"]
        # {
        #     "calendar_2022-12-12.csv": "input/calendar_2022-12-12.csv",
        #     "inventory_2022-12-12.csv": "input/inventory_2022-12-12.csv",
        #     "inventory_2022-12-13.csv": "input/inventory_2022-12-13.csv",
        #     "product_2022-12-12.csv": "input/product_2022-12-12.csv",
        #     "sales_2022-12-12.csv": "input/sales_2022-12-12.csv",
        #     "store_2022-12-12.csv": "input/store_2022-12-12.csv"
        # }   
        self.s3_file_dict = s3_file_dict
        
    def checkFiles(self):
        for each_today_required_file in self.today_required_file_list:
            if each_today_required_file not in self.s3_file_dict:
                return False
        return True

def lambda_handler(event, context):
    validator = Validator()

    # scan S3 bucket
    if validator.checkFiles():
        # s3_file_url = ['s3://' + '<your s3 bucket>/' + a for a in s3_file_list]
        # table_name = [a[:-15] for a in s3_file_list]   

        # data = json.dumps({'conf':{a:b for a in table_name for b in s3_file_url}})
        # # send signal to Airflow    
        # endpoint= 'http://<your airflow EC2 url>/api/experimental/dags/<your airflow dag name>/dag_runs'

        # subprocess.run(['curl', '-X', 'POST', endpoint, '--insecure', '--data', data])
        print('File are send to Airflow')
    else:
        send_email()