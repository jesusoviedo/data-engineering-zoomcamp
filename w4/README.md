# Semana 

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
pipenv install pandas polars dbt-core dbt-postgres
```

```bash
pipenv install --dev notebook
```

Carpeta para codigo DBT local (si prefiere trabajar localmente)

```bash
mkdir local_dbt
```


## 3. Creación y inicialización del entorno

### **DBT + BigQuery + GitHub**

Paso 1: necesitamos una cuenta de servicio con permisos de `BigQuery Data Editor`, `BigQuery Job User` y `BigQuery User` 

Paso 2: Descargar el archivo .json

Paso 3: Crear una cuenta en DBT cloud

Paso 4: Colocar un nombre del proyecto

Paso 5: Seleccionar Bigquery coomo almacen de datos y subir el archivo levantado en el paso 2

Paso 6: Configure el entorno de desarrollo indicando el nombre del dataset

Paso 7: Configure el repositorio de GitHub u otro que quiera utilizar para el proyecto de DBT

Paso 8: Inicializa tu proyecto DBT


### **DBT(local) + PostgreSQL(local) + GitHub**

Paso 1: Contar con una instancia de PostgreSQL levantada

Paso 2: Instalar DBT localmente, no olvidarse de la dependencia para poder conectarse con PostgreSQL

```bash
pipenv install dbt-core dbt-postgres
```

Paso 3: Crear un archivo profiles.yml en la carpeta `local_dbt` para definir el consumo del [PostgreSQL](https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup) de manera local


```bash
local_nyc_tripdata:
  target: dev
  outputs:
    dev:
      type: postgres
      host: [hostname]
      user: [username]
      password: [password]
      port: [port]
      dbname: [database name] # or database instead of dbname
      schema: [dbt schema]
      threads: [optional, 1 or more]
      [keepalives_idle](#keepalives_idle): 0 # default 0, indicating the system default. See below
      connect_timeout: 10 # default 10 seconds
      [retries](#retries): 1  # default 1 retry on error/timeout when opening connections
      [search_path](#search_path): [optional, override the default postgres search_path]
      [role](#role): [optional, set the role dbt assumes when executing queries]
      [sslmode](#sslmode): [optional, set the sslmode used to connect to the database]
      [sslcert](#sslcert): [optional, set the sslcert to control the certifcate file location]
      [sslkey](#sslkey): [optional, set the sslkey to control the location of the private key]
      [sslrootcert](#sslrootcert): [optional, set the sslrootcert config value to a new file path in order to customize the file location that contain root certificates]
```

Paso 4: estableces la ubicacion de los profiles de DBT

```bash
export DBT_PROFILES_DIR=path_home/data-engineering-zoomcamp/w4/local_dbt
```


Paso 5: Inicializar un proyecto DBT en la carpeta local definida anteriormente

```bash
pipenv shell
cd local_dbt
dbt init local_nyc_tripdata
```

Paso 6: Verificar el archivo dbt_proyect.yml para verificar la estructura del proyecto y cambiar el perfil si es necesario. 

Paso 7: Validar la conectividad con PostgreSQL

```bash
cd local_nyc_tripdata
dbt debug
```



## 4. Data Load Tool (DBT)



###  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*

- *[Que es DBT?](https://docs.getdbt.com/docs/introduction)*



https://www.getdbt.com/blog/what-is-analytics-engineering
https://www.datacamp.com/blog/what-is-an-analytics-engineer-everything-you-need-to-know
https://www.ibm.com/think/topics/data-engineer-data-vs-data-scientist-vs-analytics-engineer















