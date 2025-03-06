## Homework 4


### 1. Entorno de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install pandas dlt[bigquery] google-cloud-bigquery-storage 
```

```bash
pipenv install --dev notebook 
```

Carpeta de notebooks

```bash
mkdir notebooks
```

Carpeta de script

```bash
mkdir script
```

### 2. Cargando datos en Bigquery y GCS


Carpeta de configuraciones de DLT

```bash
mkdir .dlt
```

Paso 1: necesitamos una cuenta de servicio con permisos de `Storage Admin`, `Storage Object Creator`, `BigQuery Data Editor`, `BigQuery Job User` y `BigQuery Read Session User`

Paso 2: descargar el archivo con el nombre de `gcp_credentials.json` en la carpeta `.dlt`

Paso 3: definir variable de entorno que Google Cloud SDK y bibliotecas que buscan por defecto 

*Puedes ejecutar este comando estando en la carpeta raiz que contiene el archivo:*

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gcp_credentials.json"
```

*Puedes verificar con el siguiente comando:*

```bash
echo $GOOGLE_APPLICATION_CREDENTIALS
```

*Para que la variable persista entre sesiones, agrega la línea anterior a tu archivo de configuración de shell (por ejemplo, .bashrc, .zshrc o .bash_profile)*


Paso 4: crear archivo secrets en la carpeta `.dlt`:

```bash
touch secrets.toml
```

Paso 5: copiar el `project_id`, `private_key` y `client_email` del archivo .json
```bash
[destination]
location = "US"

[destination.credentials]
project_id ="project_id"  
private_key = "private_key" 
client_email = "client_email"  
```

Paso 6: ejecutar el script (se debe pasar el nombre del bucket, tipo de taxy, año, mes inicio y mes fin y como argumentos):

Ejemplos:

```bash
python script/load_data.py \
-bk data_enginnering_rj92_hw4 \
-t green \
-y 2019 \
-i 1 \
-f 12
```

```bash
python script/load_data.py \
-bk data_enginnering_rj92_hw4 \
-t yellow \
-y 2019 \
-i 1 \
-f 12
```

```bash
python script/load_data.py \
-bk data_enginnering_rj92_hw4 \
-t fhv \
-y 2019 \
-i 1 \
-f 12
```

### 3 Iniciar DBT local + BigQuery

Paso 1: Necesitamos una cuenta de servicio con permisos de `BigQuery Data Editor`, `BigQuery Job User` y `BigQuery User`

Paso 2: Descarga el archivo con el nombre de `gcp_credentials.json` en la carpeta `credentials`

*Observación: no debes guardar el contenido del archivo `gcp_credentials.json` en GitHub*

Paso 3: Instala DBT localmente, no olvidarse de la dependencia para poder conectarse con PostgreSQL

```bash
pipenv install dbt-core dbt-bigquery 
```

Paso 4: Inicializa un proyecto DBT

```bash
pipenv shell
dbt init dbt_nyc_hw4
```

Paso 5: Sigue las instrucciones para crear el `profiles.yml`

*Se realizaran algunas consultas como:*
- *Which database would you like to use?*
- *Desired authentication method option*
- *keyfile (/path/to/bigquery/keyfile.json)*
- *project (GCP project id)*
- *dataset (the name of your dbt dataset)*
- *threads (1 or more)*
- *job_execution_timeout_seconds*
- *Desired location option*


Paso 6: Abre el archivo `dbt_proyect.yml` para verificar la estructura del proyecto. 

Paso 7: Valida la conectividad con BigQuery

```bash
cd dbt_nyc_hw4
dbt debug
```


### 4 Iniciar notebook

Iniciar notebook

```bash
pipenv shell
```

```bash
jupyter notebook &
```


### 3. Desarrollo de la tarea
Descarga el archivo **[Homework4.ipynb](./notebooks/Homework4.ipynb)** en la carpeta de notebooks para revisar los detalles de la solución de la tarea