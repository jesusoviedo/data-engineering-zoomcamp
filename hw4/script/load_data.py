import os
import argparse
import dlt
import pandas as pd
import time
import gzip
import requests
from io import BytesIO 
from google.cloud import storage
from datetime import datetime
from tqdm import tqdm

GREEN = "\033[92m"
RESET = "\033[0m"

YEAR = datetime.now().year
TYPE_TAXY = ('green', 'yellow', 'fhv')
DICT_TABLE_NAME = {'green': 'green_tripdata', 'yellow': 'yellow_tripdata_test', 'fhv': 'fhv_data'}
TYPE_TABLE_ORIGIN_PARQUET = ()
TYPE_TABLE_ORIGIN_CSV_GZ = ('fhv_data', 'green_tripdata', 'yellow_tripdata_test')
BASE_URL = ("https://d37ci6vzurychx.cloudfront.net/trip-data", "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/")


def params():
    parser = argparse.ArgumentParser(description='Carga de archivos usando DLT')
    parser.add_argument('-bk', '--bucket_name', type=str, help='Nombre del bucket')
    parser.add_argument('-t', '--type_taxi', type=str, choices=[TYPE_TAXY[0], TYPE_TAXY[1], TYPE_TAXY[2]], help='Tipo de dataset')
    parser.add_argument('-y', '--year', type=int, choices=range(2015, YEAR + 1), help='Año del archivo')
    parser.add_argument('-i', '--month_ini', type=int, choices=range(1, 13), help='Mes inicio')
    parser.add_argument('-f', '--month_fin', type=int, choices=range(1, 13), help='Mes fin')
    parser.add_argument('-d', '--debug', type=bool, choices=[True, False], default=False, help='Activar debug')
    
    return parser.parse_args()


def create_bucket(bucket_name, location="US"):
    client = storage.Client()
    bucket = client.lookup_bucket(bucket_name)

    if bucket:
        print(f"Bucket '{bucket_name}' exist in {bucket.location}.")
    else:
        new_bucket = client.create_bucket(bucket_name, location=location)
    
        print(f"Bucket {new_bucket.name} create in {new_bucket.location}")


def init_config(bucket_name, activate_debug):
    
    os.environ["BUCKET_URL"] = f"gs://{bucket_name}" 
    dlt.config

    if activate_debug:
        os.environ["DLT_LOG_LEVEL"] = "DEBUG"
        logging.basicConfig(level=logging.DEBUG)
    

def get_url(year, month, type_taxi, table_name):
    
    if table_name in TYPE_TABLE_ORIGIN_PARQUET:
        url = f"{BASE_URL[0]}/{type_taxi}_tripdata_{year}-{month}.parquet"
    elif table_name in TYPE_TABLE_ORIGIN_CSV_GZ:
        url = f"{BASE_URL[1]}/{type_taxi}/{type_taxi}_tripdata_{year}-{month}.csv.gz" 
    else:
        raise Exception(f"Type taxi no support {type_taxi}")

    return url

     
def force_data_types(chunck, type_taxi):
    
    if type_taxi == TYPE_TAXY[0]:
        return chunck.astype({
            "VendorID": "Int64",
            "lpep_pickup_datetime": "datetime64[ns]",
            "lpep_dropoff_datetime": "datetime64[ns]",
            "store_and_fwd_flag": "string",
            "RatecodeID": "Int64",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "ehail_fee": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "payment_type": "Int64",
            "trip_type": "Int64",
            "congestion_surcharge": "float64"})

    elif type_taxi == TYPE_TAXY[1]:
        return chunck.astype({
           "VendorID": "Int64",
            "tpep_pickup_datetime": "datetime64[ns]",
            "tpep_dropoff_datetime": "datetime64[ns]",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "string",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64"})
    
    elif type_taxi == TYPE_TAXY[2]:
        return chunck.astype({
            "dispatching_base_num": "string",
            "pickup_datetime": "datetime64[ns]",
            "dropOff_datetime": "datetime64[ns]",
            "PUlocationID": "Int64",
            "DOlocationID": "Int64",
            "SR_Flag": "float64",
            "Affiliated_base_number": "string"
        })
    
    else:
        return chunck


def load_dataset_in_bigquery(year, month, type_taxi, table_name):

    @dlt.resource(name=f"{table_name}", write_disposition="append")
    def resource_nyc_taxi_trip(url, type_taxi, chunk_size=5000):

        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        buffer = BytesIO()

        with tqdm(desc="Downloading", total=total_size, unit="B", unit_scale=True, unit_divisor=1024) as bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                buffer.write(chunk)
                bar.update(len(chunk))
        buffer.seek(0)

        with gzip.GzipFile(fileobj=buffer, mode="rb") as gz_file:
            for chunk in pd.read_csv(gz_file, chunksize=chunk_size, low_memory=False): 
                yield force_data_types(chunk, type_taxi)



    url = get_url(year, month, type_taxi, table_name)

    pipeline = dlt.pipeline(
        pipeline_name='pipeline_insert_bigquery_staging_filesystem',
        destination='bigquery',
        staging='filesystem',
        dataset_name='trip_data_nyc',
        dev_mode= False
    )
    
    load_info = pipeline.run(
        resource_nyc_taxi_trip(url, type_taxi, chunk_size=100000), 
        loader_file_format="parquet"
    )

    print(load_info)


if __name__ == "__main__":
    
    args = params()
    init_config(args.bucket_name, args.debug)
    create_bucket(args.bucket_name)

    list_month = [f"{i:02d}" for i in range(args.month_ini, args.month_fin + 1)]
    table_name =  DICT_TABLE_NAME.get(args.type_taxi)

    for month in list_month:
        print("=" * 70)
        print(f"{GREEN}Starting the process for {args.year}-{month}...{RESET}")
        inicio = time.time()

        load_dataset_in_bigquery(args.year, month, args.type_taxi, table_name)

        fin = time.time()
        duration = fin - inicio
        minutes = duration // 60
        print(f"{GREEN}The task took {minutes:.0f} minutes and {duration - (minutes * 60):.4f} seconds to execute{RESET}")
        print("=" * 70)