## Semana 1

### 1. Comandos básicos

Descargar archivo

```bash
wget <URL>
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


### 2. Entornos de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.10
```

```bash
pipenv install pandas
```

```bash
pipenv install --dev pgcli
```

```bash
pipenv install --dev notebook
```

### 3. Docker

**Contenedor de PostgresSQL**

```bash
mkdir postgres_data
```

```bash
docker run -it \
  -e POSTGRES_USER="w1dataengi" \
  -e POSTGRES_PASSWORD="w1d@taengi" \
  -e POSTGRES_DB="ny_taxi" \
  -p 5432:5432 \
  -v $(pwd)/postgres_data:/var/lib/postgresql/data \
  postgres:17.2
```

```bash
pipenv shell
```

*En PostgreSQL, puedes usar el carácter %40 para escapar @ en una cadena de conexión*

```bash
pgcli postgres://w1dataengi:w1d%40taengi@localhost:5432/ny_taxi
```


**Iniciar notebook**

```bash
pipenv shell
```

```bash
jupyter notebook
```

Convertir notebook a script de python

```bash
jupyter nbconvert --to=script <notebook>
```

**Contenedor de PgAdmin**

```bash
docker pull dpage/pgadmin4
```

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="w1@dataengi.com" \
  -e PGADMIN_DEFAULT_PASSWORD="w1dataengi" \
  -p 8080:80 \
  dpage/pgadmin4
```

**Redes en Docker**

```bash
docker network create postgres-network
```

```bash
docker network ls
```

```bash
docker run -it \
  -e POSTGRES_USER="w1dataengi" \
  -e POSTGRES_PASSWORD="w1d@taengi" \
  -e POSTGRES_DB="ny_taxi" \
  -p 5432:5432 \
  -v $(pwd)/postgres_data:/var/lib/postgresql/data \
  --network=postgres-network \
  --name postgres-db \
  postgres:17.2
```

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="w1@dataengi.com" \
  -e PGADMIN_DEFAULT_PASSWORD="w1dataengi" \
  -p 8080:80 \
  --network=postgres-network \
  --name postgres-ui \
  dpage/pgadmin4
```

**Script de ingesta de datos**

```bash
python ingest_data.py  \
  -u=w1dataengi  \
  -p=w1d%40taengi  \
  -ht=localhost  \
  -po=5432  \
  -db=ny_taxi  \
  -tb=ny_taxi_data  \
  -uf=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```

**Ingesta de datos usando Docker**

```bash
docker build -t trip_ingest:1 .
```

```bash
docker run -it \
  --network=postgres-network \
  trip_ingest:1 \
  -u=w1dataengi  \
  -p=w1d%40taengi  \
  -ht=postgres-db  \
  -po=5432  \
  -db=ny_taxi  \
  -tb=ny_taxi_data  \
  -uf=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```

**Docker Compose**

Construir

```bash
docker-compose build
```

Iniciar

```bash
docker-compose up
```

Inicar en segundo plano

```bash
docker-compose up -d
```

Detener

```bash
docker-compose down
```

### 4. SQL

```sql
SELECT * FROM ny_taxi_data
```

```sql
SELECT * FROM zone_taxi_data
```

INNER JOIN

```sql
SELECT *  
FROM ny_taxi_data 
INNER JOIN zone_taxi_data 
ON ny_taxi_data."DOLocationID" = zone_taxi_data."LocationID"
LIMIT 100
```

```sql
SELECT 
trip.fare_amount,
PUZON."Zone" AS ZonePickup,
DOZON."Zone" AS ZoneDropoff
FROM ny_taxi_data trip
INNER JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
INNER JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
LIMIT 100
```

INNER JOIN + ORDER BY

```sql
SELECT 
trip.fare_amount,
PUZON."Zone" AS ZonePickup,
DOZON."Zone" AS ZoneDropoff
FROM ny_taxi_data trip
INNER JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
INNER JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
ORDER BY trip.fare_amount DESC
LIMIT 1000
```

ZONE NONE

```sql
SELECT * 
FROM zone_taxi_data 
WHERE "LocationID" = 831
```

```sql
DELETE 
FROM zone_taxi_data
WHERE zone_taxi_data."LocationID" = 92
```


LEFT JOIN + ORDER BY

```sql
SELECT 
trip.fare_amount,
PUZON."Zone" AS ZonePickup,
DOZON."Zone" AS ZoneDropoff
FROM ny_taxi_data trip
LEFT JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
LEFT JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
ORDER BY trip.fare_amount DESC
LIMIT 1000
```

```sql
SELECT 
trip.fare_amount,
PUZON."Zone" AS ZonePickup,
DOZON."Zone" AS ZoneDropoff
FROM ny_taxi_data trip
LEFT JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
LEFT JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
WHERE PUZON."Zone" IS NULL
OR DOZON."Zone" IS NULL
ORDER BY trip.fare_amount DESC
```

RIGHT JOIN + ORDER BY

```sqL
SELECT 
trip.fare_amount,
PUZON."Zone" AS ZonePickup,
DOZON."Zone" AS ZoneDropoff
FROM ny_taxi_data trip
RIGHT JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
RIGHT JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
ORDER BY trip.fare_amount DESC
```

```sqL
SELECT 
trip.fare_amount,
PUZON."Zone" AS ZonePickup,
DOZON."Zone" AS ZoneDropoff
FROM ny_taxi_data trip
RIGHT JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
RIGHT JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
WHERE PUZON."Zone" IS NULL
AND DOZON."Zone" IS NULL
ORDER BY trip.fare_amount DESC
```

GROUP BY + ORDER BY

```sqL
SELECT 
COUNT(1) Cantidad,
SUM(trip.fare_amount) AS TotalAmount,
PUZON."Zone" AS ZonePickup
FROM ny_taxi_data trip
INNER JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
INNER JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
GROUP BY PUZON."Zone"
ORDER BY TotalAmount DESC
```

```sqL
SELECT 
PUZON."Zone" AS ZonePickup,
COUNT(1) Cantidad,
SUM(trip.fare_amount) AS TotalAmount,
MIN(trip.fare_amount) AS MinAmount,
MAX(trip.fare_amount) AS MaxAmount
FROM ny_taxi_data trip
INNER JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
INNER JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
GROUP BY PUZON."Zone"
ORDER BY TotalAmount DESC
```

```sqL
SELECT 
PUZON."Zone" AS ZonePickup,
COUNT(1) Cantidad,
SUM(trip.fare_amount) AS TotalAmount,
MIN(trip.fare_amount) AS MinAmount,
MAX(trip.fare_amount) AS MaxAmount
FROM ny_taxi_data trip
INNER JOIN zone_taxi_data AS DOZON
ON trip."DOLocationID" = DOZON."LocationID"
INNER JOIN zone_taxi_data AS PUZON
ON trip."PULocationID" = PUZON."LocationID"
GROUP BY PUZON."Zone"
ORDER BY Cantidad DESC
```

### 5. Terraform

Setear variable de entorno para indicar credenciales de GCP (varia de acuerdo al proveedor Cloud a utilizar)

```bash
export GOOGLE_CREDENTIALS='/home/joviedo/data-engineering-zoomcamp/w1/keys-cloud/gcp_credentials'
```

Visualizar variable de entorno
```bash
echo $GOOGLE_CREDENTIALS
```

Obtener proveedores y configurar el backend
```bash
terraform init
```

Formatear archivos de terraform
```bash
terraform fmt
```

Validacion de sintasis de terraform
```bash
terraform validate
```

Plan de ejecución 
```bash
terraform plan
```

Aplicacion del plan (agregar/modificar/eliminar recursos)
```bash
terraform apply
```

Aplicacion del plan con autoaprobación
```bash
terraform apply -auto-approve
```

Eliminación de todos los recursos
```bash
terraform destroy
```


Eliminación de todos los recursos con autoaprobación
```bash
terraform destroy -auto-approve
```


####  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- *[docker build](https://docs.docker.com/get-started/docker-concepts/building-images/build-tag-and-publish-an-image/)* 
- *[docker run](https://docs.docker.com/reference/cli/docker/container/run/)* 
- *[docker network](https://docs.docker.com/reference/cli/docker/network/)*
- *[pipenv](https://pipenv-es.readthedocs.io/es/latest/)*
- *[pandas](https://pandas.pydata.org/)*
- *[pgcli](https://www.pgcli.com/)*
- *[postgres](https://hub.docker.com/_/postgres)* 
- *[jupyter notebook](https://jupyter.org/install)*
- *[pgadmin](https://www.pgadmin.org/docs/pgadmin4/development/container_deployment.html)* 
- *[comandos linux](https://www.dreamhost.com/blog/es/comandos-linux-que-debes-conocer/)* 
- *[sql join](https://www.postgresql.org/docs/current/tutorial-join.html)*
- *[sql group by](https://www.postgresql.org/docs/current/tutorial-agg.html)*
- *[terraform](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/infrastructure-as-code)*
- *[terraform install ](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)*
- *[terraform providers](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-build)*
- *[terraform apply](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-change)*
- *[terraform destroy](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-destroy)*
- *[terraform variables](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-variables)*
- *[terraform output values](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-outputs)*
- *[provider google](https://registry.terraform.io/providers/hashicorp/google/6.14.1/docs/guides/provider_reference)*
 