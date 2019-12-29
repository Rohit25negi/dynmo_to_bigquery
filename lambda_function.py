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
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
]
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    client.load_table_from_file(file_obj, dataset_ref.table(os.environ['TABLE_NAME'] ), job_config=job_config) 


def prepare_where_cluse(props):
    where_clause = []

    for prop_key, prop_value in props.items():
        where_clause.append((prop_key, list(prop_value.values())[0]))

    where_clause = ["CAST({} as STRING) = '{}'".format(clause[0], clause[1]) for clause in where_clause]

    return ' AND '.join(where_clause)


def handle_insert_event(record):
    data = get_header_data(record)
    output = io.BytesIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerows(data)
    output.seek(0)
    put_data_to_google_drive(output)


def handle_delete_event(record):
    qualified_tablename = "{}.{}.{}".format('ipython-notebook-dev-244005',
                                            os.environ['DATASET_ID'],
                                            os.environ['TABLE_NAME'])
    query = """
    DELETE FROM `{tablename}` bigquery_table
    WHERE {where_clause}
    """.format(tablename = qualified_tablename, where_clause=prepare_where_cluse(record))
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()


def handle_update_event(record):
    qualified_tablename = "{}.{}.{}".format('ipython-notebook-dev-244005',
                                            os.environ['DATASET_ID'],
                                            os.environ['TABLE_NAME'])

    handle_delete_event(record['OldImage'])
    handle_insert_event(record['NewImage'])


def lambda_handler(event, context):
    try:
        for record in event['Records']:
            print(record['eventName'])
            if record['eventName'] == 'INSERT':
                handle_insert_event(record['dynamodb']['NewImage'])
            elif record['eventName'] == 'MODIFY':
                handle_update_event(record['dynamodb'])
            elif record['eventName'] == 'REMOVE':
                handle_delete_event(record['dynamodb']['OldImage'])
    except Exception as ex:
        print(ex)
    return 'Successfully processed {} records.'
