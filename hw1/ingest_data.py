#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

tup_tipo_tabla = ('trip_data', 'zone_trip')

def params():
    parser = argparse.ArgumentParser(description='Ingesta de datos de TLC Trip Record Data a postgresql')

    parser.add_argument('-u', '--user', type=str, help='Usuario de postgresql')
    parser.add_argument('-p', '--password', type=str, help='Contraseña de postgresql')
    parser.add_argument('-ht', '--host', type=str, help='Host de postgresql')
    parser.add_argument('-po', '--port', type=int, help='Puerto de postgresql')
    parser.add_argument('-db', '--database', type=str, help='DB de postgresql')
    parser.add_argument('-tb', '--table', type=str, help='Tabla de postgresql')
    parser.add_argument('-tt', '--typetable', type=str, choices=[tup_tipo_tabla[0], tup_tipo_tabla[1]], help='Tipo de tabla de postgresql')
    parser.add_argument('-uf', '--urlarchivo', type=str, help='URL del archivo csv')

    return parser.parse_args()


def get_file(url, tipo_tabla):
    carpeta_destino = "../data"
    os.makedirs(carpeta_destino, exist_ok=True)

    if url.endswith(".csv.gz"):
        csv_name = "trip_data.csv.gz" if tipo_tabla == tup_tipo_tabla[0] else "zone_data.csv.gz"
    else:
        csv_name = "trip_data.csv" if tipo_tabla ==  tup_tipo_tabla[0] else "zone_data.csv"

    ruta_completa = os.path.join(carpeta_destino, csv_name)
    os.system(f"wget {url} -O {ruta_completa}")

    return ruta_completa


def init_table(data, tabla, conexion):

    data.lpep_pickup_datetime = pd.to_datetime(data.lpep_pickup_datetime)
    data.lpep_dropoff_datetime = pd.to_datetime(data.lpep_dropoff_datetime)
    data.head(n=0).to_sql(name=f"{tabla}", con=conexion, if_exists='replace')


def insert_data(df_data, tabla, tipo_tabla, conexion):

    if tipo_tabla == tup_tipo_tabla[0]:
        for df in df_data: 
            t_start = time()
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=f'{tabla}', con=conexion, if_exists='append')
            t_end = time()
            print(f'Bloque de datos insertados en {(t_end - t_start):.3f} segundos...')
    else:
        t_start = time()
        df_data.to_sql(name=f'{tabla}', con=conexion, if_exists='replace')
        t_end = time()
        print(f'Datos insertados en {(t_end - t_start):.3f} segundos...')

    print(f'Proceso finalizado correctamente...')


def main(args):
    csv_name = get_file(args.urlarchivo, args.typetable)
    engine = create_engine(f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}")
    
    if args.typetable == tup_tipo_tabla[0]:
        dataframe = pd.read_csv(f"{csv_name}", nrows=1, low_memory=False)
        init_table(dataframe, args.table, engine)

        df_iter = pd.read_csv(f"{csv_name}", iterator=True, chunksize=100000, low_memory=False)
        insert_data(df_iter, args.table, args.typetable, engine)
    else:
        dataframe = pd.read_csv(f"{csv_name}")
        insert_data(dataframe, args.table, args.typetable, engine)

if __name__ == '__main__':
    args = params()
    main(args)
