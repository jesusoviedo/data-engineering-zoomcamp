## Homework 6


### 1. Entorno de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install kafka-python notebook pandas
```

Carpeta de notebooks

```bash
mkdir notebooks
```

Carpeta de data

```bash
mkdir data
```

*Observación: es necesario tener configurados y en funcionamiento todos los componentes requeridos para que Kafka, Flink y PostgreSQL interactúen correctamente. Puedes revisar los detalles en el [README](../w6/README.md) de la semana 6.*


### 2. Descargar datos

Descargar los datos de los viajes amarillos del periodo 2024-10 en la carpeta data
```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz -O data/green_tripdata_2019-10.csv.gz
```

Descomprimir los datos
```bash
gunzip data/green_tripdata_2019-10.csv.gz
```


### 3. Iniciar notebook

Iniciar notebook

```bash
pipenv shell
```

```bash
jupyter notebook &
```


### 3. Desarrollo de la tarea
Descarga el archivo **[Homework6.ipynb](./notebooks/Homework6.ipynb)** en la carpeta de notebooks para revisar los detalles de la solución de la tarea

*Comando para detener todos los servicios de notebook:*

```bash
pkill -f jupyter
```