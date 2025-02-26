# Workshop

## 1. Comandos básicos

Descargar archivo

```bash
wget <URL>
```

```bash
curl -o <archivo_destino>  <URL>
```

Descomprimir archivo

```bash
gunzip <archivo>.gz
```

Ver contenido archivo

```bash
less <archivo>

head -n 25 <archivo>
```

Mover archivo

```bash
mv <origen> <destino>
```

Eliminr archivo

```bash
rm <archivo>
```

Renombrar archivo

```bash
mv <nombre_archivo_actual> <nombre_archivo_nuevo>
```


## 2. Entornos de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install pandas polars pyarrow duckdb dlt[duckdb] dlt[bigquery] dlt[filesystem]
```


```bash
pipenv install --dev notebook
```

Carpeta de script

```bash
mkdir script
```

Carpeta de credenciales
```bash
mkdir .dlt
```


## 3. DuckDB

**Usando DuckDB**

Ejemplo simple
```bash
pipenv run python ./script/duckdb_1.py
```

Creando, insertando y mostrando resultados de una tabla
```bash
pipenv run python ./script/duckdb_2.py
```

Usando json como input para una tabla
```bash
pipenv run python ./script/duckdb_3.py
```

DuckDB + Pandas
```bash
pipenv run python ./script/duckdb_4.py
```

DuckDB + Polars
```bash
pipenv run python ./script/duckdb_5.py
```

DuckDB + Pyarrow
```bash
pipenv run python ./script/duckdb_6.py
```

## 4. Data Load Tool (DLT)

[![Cargando datos en BigQuery](https://img.youtube.com/vi/pgJWP_xqO1g/maxresdefault.jpg)](https://www.youtube.com/live/pgJWP_xqO1g)

**Primer ETL con DLT**

```bash
pipenv run python ./script/dlt_1.py
```

**Extrayendo datos con DLT**

Podemos mejorar la manera de extraer datos con DLT

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


def paginated_getter():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(   
            base_page=1,   
            total_path=None    
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    
        yield page   

for page_data in paginated_getter():
    print(page_data)
```


**DLT normaliza datos automáticamente**

No nos debemos preocupar por la normalización cuando trabajamos con datos no estructurados.

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_data",
    destination="duckdb",
    dataset_name="taxi_rides",
)

info = pipeline.run(data, table_name="rides", write_disposition="replace")

print(info)

print(pipeline.last_trace)
```

**Extracción, Normalización y Carga de datos con DLT**

Uniendo todas las partes para un ELT más completo en DLT

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


@dlt.resource(name="rides")  
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):  
        yield page  


pipeline = dlt.pipeline(destination="duckdb")

load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)

pipeline.dataset(dataset_type="default").rides.df()
```

```bash
pipenv run python ./script/dlt_2.py
```

**Ejemplo usando API de Rick and Morty + BigQuery**

Paso 1: necesitamos una cuenta de servicio con permisos de `BigQuery Data Editor`, `BigQuery Job User` y `BigQuery Read Session User`

Paso 2: descargar el archivo .json

Paso 3: crear archivo secrets en la carpeta `.dlt`:

```bash
touch secrets.toml
```

Paso 4: copiar el `project_id`, `private_key` y `client_email` del archivo .json
```bash
[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id ="project_id"  
private_key = "private_key" 
client_email = "client_email"  
```

Paso 5: ejecutar el script:

```bash
pipenv run python script/dlt_3.py
```

*Observación: no debes guardar el contenido del archivo `secrets.toml` en GitHub*

**Ejemplo usando API de trip data + Bucket GCP**

Paso 1: necesitamos una cuenta de servicio con permisos de `Storage Admin` y `Storage Object Creator`

Paso 2: descargar el archivo .json

Paso 3: definir variable de entorno que Google Cloud SDK y bibliotecas que buscan por defecto 

```bash
export GOOGLE_APPLICATION_CREDENTIALS="ruta/al/archivo/credenciales.json"
```

*Para que la variable persista entre sesiones, agrega la línea anterior a tu archivo de configuración de shell (por ejemplo, .bashrc, .zshrc o .bash_profile)*

Paso 4: crear archivo secrets en la carpeta `.dlt`:

```bash
touch secrets.toml
```

Paso 5: copiar el `project_id`, `private_key` y `client_email` del archivo .json
```bash
[destination.bigquery]
location = "US"

[destination.bigquery.credentials]
project_id ="project_id"  
private_key = "private_key" 
client_email = "client_email"  
```

Paso 5: ejecutar el script (se debe pasar el nombre del bucket, tipo de taxy, año, mes inicio y mes fin y como argumentos):

```bash
python script/dlt_4.py \
-bk data_enginnering_rj92_wk_2025 \
-t green \
-y 2024 \
-i 1 \
-f 7
```

*Observación: no debes guardar el contenido del archivo secrets.toml en GitHub*


###  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- [DLT - Data ingestion workshop](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/workshops/dlt/data_ingestion_workshop.md)
- *[DuckDB - Python API](https://duckdb.org/docs/clients/python/overview.html)*
- *[DuckDB - Documentación](https://duckdb.org/docs/index)*
- *[DuckDB - GitHub](https://github.com/duckdb/duckdb)*
- *[DLT - GitHub](https://github.com/dlt-hub/dlt)*
- *[DLT - Getting started](https://dlthub.com/docs/intro)*
- *[DLT - How dlt works](https://dlthub.com/docs/reference/explainers/how-dlt-works)*
- *[DLT - Core concepts](https://dlthub.com/docs/dlt-ecosystem/verified-sources/)*
- *[DLT - Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/)*
- *[DLT - Destination duckdb](https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb)*
- *[DLT - Destination bigquery](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery)*
- *[DLT - Destination filesystem](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem)*
- *[DLT - Using dlt](https://dlthub.com/docs/general-usage/)*
















