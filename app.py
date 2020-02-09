import os
import requests
import time
import datetime
from os import environ
from google.cloud import storage
import airflow
from airflow.operators import bash_operator
from google.cloud import bigquery

from os import listdir
from os.path import isfile, join


def fetch_jsons(entry):
    path, uri = entry
    start = time.time()
    if not os.path.exists(path):
        r = requests.get(uri, stream=True)
        if r.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in r:
                    f.write(chunk)

    print(time.time() - start)
    return path


def gcp_bucket_transfer():

    bucketName = environ.get('GCP_BUCKET_NAME')
    localFolder = environ.get('LOCAL_FOLDER')

    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucketName)
    bucket = storage_client.bucket(bucketName)
    print("Bucket {} created.".format(bucket.name))

    files = [f for f in listdir(localFolder) if isfile(join(localFolder, f))]
    for file in files:
        localFile = os.path.join(localFolder, file)
        blob = bucket.blob(file)
        blob.upload_from_filename(localFile)
        print(blob.public_url)

    return 'Uploaded {} to {} bucket with ()'.format(files, bucketName)


def dagEtlGSC2BQ():

    YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

    default_args = {
        'owner': 'Composer Example',
        'depends_on_past': False,
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'start_date': YESTERDAY,
    }

    with airflow.DAG(
            'composer_sample_dag',
            'catchup=False',
            default_args=default_args,
            schedule_interval=datetime.timedelta(days=1)) as dag:

        # Print the dag_run id from the Airflow logs
        print_dag_run_conf = bash_operator.BashOperator(
            task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')


def etlBQFromGCS():

    client = bigquery.Client()
    query_job = client.query("""
        SELECT
        CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
        view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10""")

    results = query_job.result()  # Waits for job to complete.
    for row in results:
        print("{} : {} views".format(row.url, row.view_count))


def bq_create_dataset():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_datasset_id')

    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))


def bq_create_table():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_datasset_id')

    # Prepares a reference to the table
    table_ref = dataset_ref.table('my_table_name')

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))

    
if __name__ == "__main__":

    base_url = "https://s3.amazonaws.com/data-sprints-eng-test/"
    payloads = [
        ("data-nyctaxi-trips-2009.json", base_url + "data-sample_data-nyctaxi-trips-2009-json_corrigido.json"),
        ("data-nyctaxi-trips-2010.json", base_url + "data-sample_data-nyctaxi-trips-2010-json_corrigido.json"),
        ("data-nyctaxi-trips-2011.json", base_url + "data-sample_data-nyctaxi-trips-2011-json_corrigido.json"),
        ("data-nyctaxi-trips-2012.json", base_url + "data-sample_data-nyctaxi-trips-2012-json_corrigido.json"),
        ("data-vendor_lookup.csv", base_url + "data-vendor_lookup-csv.csv"),
        ("data-payment_lookup.csv", base_url + "data-payment_lookup-csv.csv")
    ]

    # for entry in payloads:
    #    fetch_jsons(entry)

    # gcp_bucket_transfer()

    # dagEtlGSC2BQ()
    etlBQFromGCS()
