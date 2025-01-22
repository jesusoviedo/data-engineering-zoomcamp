#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def params():
    parser = argparse.ArgumentParser(description='Ingesta de datos de TLC Trip Record Data a postgresql')

    parser.add_argument('-u', '--user', type=str, help='Usuario de postgresql')
    parser.add_argument('-p', '--password', type=str, help='Contraseña de postgresql')
    parser.add_argument('-ht', '--host', type=str, help='Host de postgresql')
    parser.add_argument('-po', '--port', type=int, help='Puerto de postgresql')
    parser.add_argument('-db', '--database', type=str, help='DB de postgresql')
    parser.add_argument('-tb', '--table', type=str, help='Tabla de postgresql')
    parser.add_argument('-uf', '--urlarchivo', type=str, help='URL del archivo csv')

    return parser.parse_args()


def get_file(url):
    carpeta_destino = "output"
    os.makedirs(carpeta_destino, exist_ok=True)

    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"    

    ruta_completa = os.path.join(carpeta_destino, csv_name)
    os.system(f"wget {url} -O {ruta_completa}")

    return ruta_completa


def init_table(data, tabla, conexion):

    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)
    data.head(n=0).to_sql(name=f"{tabla}", con=conexion, if_exists='replace')


def insert_data(df_iterator, tabla, conexion):

    for df in df_iterator: 
        t_start = time()

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=f'{tabla}', con=conexion, if_exists='append')

        t_end = time()
        print(f'Bloque de datos insertados en {(t_end - t_start):.3f} segundos...')

    print(f'Proceso finalizado correctamente...')


def main(args):
    csv_name = get_file(args.urlarchivo)
    dataframe = pd.read_csv(f"{csv_name}", nrows=1)
    engine = create_engine(f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}")
    
    init_table(dataframe, args.table, engine)

    df_iter = pd.read_csv(f"{csv_name}", iterator=True, chunksize=100000)
    insert_data(df_iter, args.table, engine)


if __name__ == '__main__':
    args = params()
    main(args)
