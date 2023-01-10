import json
import csv
import boto3
# from boto3.dynamodb.table import TableResource

def create_table():
    table_name = 'climate_data'
    client = boto3.client('dynamodb')
    response = client.list_tables()
    if table_name in response['TableNames']:
        print(f'climate_data table exists in DynamoDB')
    else:
        # Get Dynamodb resource
        dynamodb = boto3.resource('dynamodb')
        # Dynamodb.create_table returns Table resource
        table = dynamodb.create_table(TableName=table_name,
            KeySchema=[
                    {
                        'AttributeName': 'year',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'date',
                        'KeyType': 'RANGE'
                    }

                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'year',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'date',
                        'AttributeType': 'S'
                    }
                ],
                ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5,
                    }
            )
        # Dynamodb.create_table is asynchronous task, wait until the table is created completely
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print('table created')

def get_year():
    s3 = boto3.client('s3')
    bucket = 'hui-lab10'
    key = 'config.json'
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body']
    config = json.loads(content.read())
    year = config['year']
    print(f"Reading S3 bucket {bucket}, year is {year}")
    return year
    
def lambda_handler(event, context):
    create_table()
    year = get_year() 
    
    try:
        s3_client = boto3.client('s3')
        dynamodb_client = boto3.client('dynamodb')
        table_name = 'climate_data'
        # when upload data.csv to S3, bucket in trigger event is hui-lab10
        bucket = event['Records'][0]['s3']['bucket']['name']
        # when upload data.csv to S3, key in trigger event is /input/2017/climate_date.csv
        key = event['Records'][0]['s3']['object']['key']

        csv_file = s3_client.get_object(Bucket=bucket,Key=key)
        record_list = csv_file['Body'].read().decode('utf-8').split('\n')
        csv_reader = csv.reader(record_list, delimiter=',',quotechar='"')
        next(csv_reader)

        for row in csv_reader:
            if str(year) in row[5]:
                print(row)               
                lon = row[0]
                lat = row[1]
                station_name = row[2]
                climateid = row[3]
                date = row[4]
                year = row[5]
                month = row[6]
                day = row[7]
                max_temp = row[9]
                min_temp = row[11]
                mean_temp = row[13]
                dynamodb_client.put_item(TableName=table_name,Item = {
                    'lon':{'S': str(lon)},
                    'lat':{'S': str(lat)},
                    'station_name':{'S': str(station_name)},
                    'climateid':{'S': str(climateid)},
                    'date':{'S': str(date)},
                    'year':{'S': str(year)},
                    'month':{'S': str(month)},
                    'day':{'S': str(day)},
                    'max_temp':{'S': str(max_temp)},
                    'min_temp':{'S': str(min_temp)},
                    'mean_temp':{'S': str(mean_temp)}
                }) 
            else:
                print('The uploaded file is not the right year')
         
    except Exception as e:
        print(str(e))
    print("Import Data to Dynamodb from S3 csv file is done")
    return {'statusCode':200, 'body':json.dumps('File uploaded to DynamoDB')}