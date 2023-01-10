import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key



def lambda_handler(event, context):

    year = event["year"]
    start_date = event["start_date"]
    end_date = event["end_date"]
    
    table_name = 'climate_data'
    client = boto3.resource('dynamodb')
    table = client.Table(table_name)

    response = table.query(
        KeyConditionExpression=Key('year').eq(year)&Key('date').between(start_date, end_date)
        )
    items = response['Items']

    item_list = []
    for item in items:
        item_list.append(item)

    df = pd.DataFrame(item_list)
    local_file = '/tmp/climate_data_selected_result.csv'
    df.to_csv(local_file)

    s3 = boto3.client('s3')
    bucket_name = 'hui-lab10'
    s3_file = 'output/climate_data_selected_result.csv'
    s3.upload_file(local_file, bucket_name, s3_file)