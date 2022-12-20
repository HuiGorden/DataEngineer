import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('--spark_name', help="spark_name")
parser.add_argument('--input_file_url', help="input file url json string")

args = parser.parse_args()
spark_name = args.spark_name
input_file_url = args.input_file_url
input_file_dict = json.loads(input_file_url)
print(spark_name, input_file_dict)