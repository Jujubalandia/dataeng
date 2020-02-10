import os
import requests
import time
from os import environ
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest

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


def bq_dataset_structure():

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(environ.get('BQ_DATASET_NAME'))

    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))


def bq_datatables_structure(table_name, schema):

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(environ.get('BQ_DATASET_NAME'))
    table_ref = dataset_ref.table(table_name)

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))


def bq_load_json_gsc(uri_file_bucket, table_name):

    client = bigquery.Client()
    dataset_id = environ.get('BQ_DATASET_NAME')

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.max_bad_records = 1000
    job_config.ignore_unknown_values = True 
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = uri_file_bucket

    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table_name),
        location="US",
        job_config=job_config,
    )

    print("Starting job {}".format(load_job.job_id))

    try:
        load_job.result()
        print("Job finished.")
        destination_table = client.get_table(table_name)
        print("Loaded {} rows.".format(destination_table.num_rows))
    except BadRequest:
        raise RuntimeError(load_job.errors)


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

    for entry in payloads:
        fetch_jsons(entry)

    gcp_bucket_transfer()

    schema_ny_trips = [
            bigquery.SchemaField('dropoff_datetime', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('dropoff_latitude', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('dropoff_longitude', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('fare_amount', 'FLOAT', mode='NULLABLE'),   
            bigquery.SchemaField('passenger_count', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('payment_type', 'STRING', mode='NULLABLE'), 
            bigquery.SchemaField('pickup_datetime', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('pickup_latitude', 'FLOAT', mode='NULLABLE'), 
            bigquery.SchemaField('pickup_longitude', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('rate_code', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('store_and_fwd_flag', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('surcharge', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('tip_amount', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('tolls_amount', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('total_amount', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('trip_distance', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('vendor_id', 'STRING', mode='NULLABLE')
    ]

    schema_data_vendor = [
            bigquery.SchemaField('vendor_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('address', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('city', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('state', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('zip', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contact', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('current', 'STRING', mode='NULLABLE')
    ]

    schema_data_payment = [
            bigquery.SchemaField('A', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('B', 'STRING', mode='NULLABLE'),
    ]

    bq_dataset_structure()
    bq_datatables_structure('ny_trips', schema_ny_trips)
    bq_datatables_structure('data_vendor', schema_data_vendor)
    bq_datatables_structure('data_payment', schema_data_payment)

    ny_2009 = "gs://bchallenger-de/data-nyctaxi-trips-2009.json"
    ny_2010 = "gs://bchallenger-de/data-nyctaxi-trips-2010.json"
    ny_2011 = "gs://bchallenger-de/data-nyctaxi-trips-2011.json"
    ny_2012 = "gs://bchallenger-de/data-nyctaxi-trips-2012.json"
    data_pay = "gs://bchallenger-de/data-payment_lookup.csv"
    data_vend = "gs://bchallenger-de/data-vendor_lookup.csv"
    
    bq_load_json_gsc(ny_2009, 'ny_trips')
    bq_load_json_gsc(ny_2010, 'ny_trips')
    bq_load_json_gsc(ny_2011, 'ny_trips')
    bq_load_json_gsc(ny_2012, 'ny_trips')
    bq_load_json_gsc(data_pay, 'data_payment')
    bq_load_json_gsc(data_vend, 'data_vendor')

