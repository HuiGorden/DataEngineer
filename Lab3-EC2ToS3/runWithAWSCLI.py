#!python3
"""
This is the main file for the py_cloud project. It can be used at any situation
"""
import subprocess
from sys import stdout
import requests
import toml
import pandas as pd
from collections import ChainMap
from dotenv import load_dotenv
import os


def read_api(url):
    response = requests.get(url)
    return response.json()


# main function
if __name__=='__main__':
    app_config = toml.load('config.toml')

    # Get data from the API
    url = app_config['api']['url']
    print(f'Reading the API -->{url}')
    data=read_api(url)
    print('API Reading Done!')

    # Construct company name dict
    print('Building the dataframe...')
    company_list = [data['results'][i]['company']['name'] for i in range(len(data['results']))]
    company_name_dict = {'company':company_list}

    # Construct location name dict
    location_list = [data['results'][i]['locations'][0]['name'] for i in range(len(data['results']))]
    location_name_dict = {'locations':location_list}
    
    # Construct job name dict
    job_list = [data['results'][i]['name'] for i in range(len(data['results']))]
    job_name_dict= {'job':job_list}

    # Construct job type dict
    job_type_list = [data['results'][i]['type'] for i in range(len(data['results']))]
    job_type_dict = {'job_type':job_type_list}

    # Construct publication date dict
    publication_date_list = [data['results'][i]['publication_date'] for i in range(len(data['results']))]
    publication_date_dict = {'publication_date':publication_date_list}

    # merge the dictionaries with ChainMap 
    data = dict(ChainMap(company_name_dict, location_name_dict, job_name_dict, job_type_dict, publication_date_dict))
    df=pd.DataFrame.from_dict(data)

    # Cut publication date to date
    df['publication_date'] = df['publication_date'].str[:10]

    # split location to city and country and drop the location column
    df['city'] = df['locations'].str.split(',').str[0]
    df['country'] = df['locations'].str.split(',').str[1]
    df.drop('locations', axis=1, inplace=True)

    # save the dataframe to jobs.csv
    df.to_csv('jobs.csv', index=False)
    print('Saved to local file called jobs.csv')

    # read secret_access_key of AWS form the .env file
    print('Prepare to upload to AWS S3...')

    # upload the csv file to AWS S3
    bucket = app_config['aws']['bucket']
    folder = app_config['aws']['folder']

    result = subprocess.run(f"aws s3 ls s3://{bucket}", shell=True, universal_newlines=True, timeout=10, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if "bucket does not exist" in result.stdout:
        print(f"{bucket} bucket is not existed, create bucket now..")
        subprocess.run(f"aws s3 mb s3://{bucket} --region us-east-1", shell=True, universal_newlines=True, timeout=10)
    else:
        print("Bucket existed, no need to create")

    subprocess.run(f"aws s3 cp jobs.csv s3://{bucket}/{folder}jobs.csv", shell=True, universal_newlines=True, timeout=10)
    print('File uploading Done!')