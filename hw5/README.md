## Homework 5


### 1. Entorno de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install pandas notebook
```

Carpeta de notebooks

```bash
mkdir notebooks
```

Carpeta de data

```bash
mkdir -p data/tmp
```

*Observación: se debe tener instalado el Spark y configurado el PySpark (ver [README](../w5/README.md) de la semana 5)*


### 2. Descargar datos

Descargar los datos de los viajes amarillos del periodo 2024-10 en la carpeta data
```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet -O data/tmp/yellow_tripdata_2024-10.parquet
```

Descargar los datos de zona
```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv -O data/tmp/taxi_zone_lookup.csv
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
Descarga el archivo **[Homework5.ipynb](./notebooks/Homework5.ipynb)** en la carpeta de notebooks para revisar los detalles de la solución de la tarea

*Comando para detener todos los servicios de notebook:*

```bash
pkill -f jupyter
```