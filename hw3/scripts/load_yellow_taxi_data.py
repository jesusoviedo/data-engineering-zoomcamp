import os
import argparse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time


global BUCKET_NAME
global bucket

#If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "../credentials/gcs.json"  
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)] 
DOWNLOAD_DIR = "../data"

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


def create_bucket(bucket_name, location="US"):

    bucket = client.lookup_bucket(bucket_name)

    if bucket:
        print(f"El bucket '{bucket_name}' ya existe en {bucket.location}.")
    else:
        new_bucket = client.create_bucket(bucket_name, location=location)
    
        print(f"Bucket {new_bucket.name} creado en {new_bucket.location}")


def params():
    parser = argparse.ArgumentParser(description='Creación de bucket y carga de archivos')
    parser.add_argument('-bk', '--bucketname', type=str, help='Nombre del bucket')

    return parser.parse_args()


if __name__ == "__main__":

    args = params()
    BUCKET_NAME = args.bucketname
    bucket = client.bucket(BUCKET_NAME)
    create_bucket(BUCKET_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))

    print("All files processed and verified.")