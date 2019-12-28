import csv
import json
import io
import os

import boto3
from google.cloud import bigquery


def get_header_data(record):
    headers = set(record.keys())
    row = []
    for header in headers:
        row.append(list(record[header].values())[0])
    return [list(headers)] + [row]


def put_data_to_google_drive(file_obj):
    client = boto3.client('dynamodb')

    # this needs an env variable to be set GOOGLE_APPLICATION_CREDENTIALS
    # value of this env variable is the path to the google credentials json file. You could make that file part
    # of this package while deployment. 
    client = bigquery.Client()
    dataset_id = os.environ['DATASET_ID']
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    client.load_table_from_file(file_obj, dataset_ref.table(os.environ['TABLE_NAME'] ), job_config=job_config) 


def handle_insert_event(record):
    data = get_header_data(record)
    output = io.BytesIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerows(data)
    output.seek(0)
    put_data_to_google_drive(output)


def lambda_handler(event, context):
    eventName = event['Records'][0]['eventName']
    record = event['Records'][0]['dynamodb']['NewImage']

    if eventName == 'INSERT':
        handle_insert_event(record)
    elif eventName == 'UPDATE':
        # TODO
        pass
    elif eventName == 'REMOVE':
        # TODO
        pass

    return 'Successfully processed {} records.'
