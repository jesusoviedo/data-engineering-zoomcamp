import os
import argparse
import dlt
import pandas as pd
from io import BytesIO
from datetime import datetime 
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from google.cloud import storage

 
YEAR = datetime.today().year
TYPE_TAXY = ('green', 'yellow')
client = storage.Client()


def params():
    parser = argparse.ArgumentParser(description='Carga de archivos usando DLT')
    parser.add_argument('-bk', '--bucket_name', type=str, help='Nombre del bucket')
    parser.add_argument('-t', '--type_taxi', type=str, choices=[TYPE_TAXY[0], TYPE_TAXY[1]], help='Tipo de taxi')
    parser.add_argument('-y', '--year', type=int, choices=range(2015, YEAR + 1), help='Año del archivo')
    parser.add_argument('-i', '--month_ini', type=int, choices=range(1, 13), help='Mes inicio')
    parser.add_argument('-f', '--month_fin', type=int, choices=range(1, 13), help='Mes fin')
    
    return parser.parse_args()


def create_bucket(bucket_name, location="US"):

    bucket = client.lookup_bucket(bucket_name)

    if bucket:
        print(f"Bucket '{bucket_name}' exist in {bucket.location}.")
    else:
        new_bucket = client.create_bucket(bucket_name, location=location)
    
        print(f"Bucket {new_bucket.name} create in {new_bucket.location}")


def download_parquet(url):
    client = RESTClient(base_url="https://d37ci6vzurychx.cloudfront.net/trip-data")

    response = client.get(url)

    if response.status_code == 200:
        df = pd.read_parquet(BytesIO(response.content))
        return df
    else:
        raise Exception(f"Failed to download file: {response.status_code}")


def load_data_dlt(bucket_name, type_taxi, year, list_month):

    os.environ["BUCKET_URL"] = f"gs://{bucket_name}" 

    @dlt.resource(name="rides", write_disposition="replace")
    def ny_taxi():

        url = f"/{type_taxi}_tripdata_{year}-{month}.parquet"
        df = download_parquet(url)

        for record in df.to_dict(orient="records"):
            yield record

    for month in list_month:
        pipeline = dlt.pipeline(
            pipeline_name='pipeline_filesystem',
            destination='filesystem',
            dataset_name='filesystem_dataset',
            dev_mode= False
        )

        load_info = pipeline.run(
            ny_taxi, 
            loader_file_format="parquet"
        )
        
        print(load_info)


if __name__ == "__main__":
    
    args = params()
    create_bucket(args.bucket_name)

    list_month = [f"{i:02d}" for i in range(args.month_ini, args.month_fin + 1)]
    load_data_dlt(args.bucket_name, args.type_taxi, args.year, list_month)