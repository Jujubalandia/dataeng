import os
import requests
import time
from os import environ
from google.cloud import storage
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










