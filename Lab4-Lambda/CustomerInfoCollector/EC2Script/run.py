#!python3
import sqlalchemy as db
import pandas as pd
import subprocess
import toml
import os
from dotenv import load_dotenv

def mysql_connect(host, user, password, database, port,schema):
    """
    Connect to the database
    """
    engine = db.create_engine(f'mysql+mysqlconnector://{user}:{password}@{database}.{host}:{port}/{schema}')
    return engine

if __name__=="__main__": 
    app_config = toml.load('config.toml')

    host=app_config['db']['host']
    port=app_config['db']['port']
    database=app_config['db']['database']
    schema=app_config['db']['schema']

    bucket=app_config['s3']['bucket']
    folder=app_config['s3']['folder']

    load_dotenv()
    user=os.getenv('user')
    password=os.getenv('password')


    sql="""
    select customerID, sum(sales) sum_sales
    from orders
    group by 1
    order by 2 desc
    limit 10;
    """
    engine = mysql_connect(host, user, password, database, port,schema)

    df = pd.read_sql(sql, con = engine)
    df[["customerID"]].head(10)
    df[["customerID"]].to_json('cus_id.json')

    subprocess.call(['aws','s3','cp','cus_id.json', f's3://{bucket}/{folder}/cus_id.json'])