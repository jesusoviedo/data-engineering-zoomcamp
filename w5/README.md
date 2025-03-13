# Semana 5

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
pipenv install pandas notebook
```

Carpeta de notebooks
```bash
mkdir notebooks
```

## 3. Instalar Spark y PySpark

**Paso 1: instalar Java**

Ir [OpenJDK](https://jdk.java.net/archive/) y descargar/instalar la versión de Linux/x64:

```bash
cd 
mkdir java11
cd java11
```

```bash
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
```

```bash
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
rm -r openjdk-11.0.2_linux-x64_bin.tar.gz
```

```bash
export JAVA_HOME="${HOME}/java11/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
```

```bash
java --version
which java
```

**Paso 2: instalar Spark**

Ir [Spark](https://spark.apache.org/downloads.html) y descargar/instalar la versión de Linux/x64:

```bash
cd 
mkdir spark
cd spark
```
```bash
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
```

```bash
tar xzfv spark-3.5.5-bin-hadoop3.tgz
rm -r spark-3.5.5-bin-hadoop3.tgz
```

```bash
export SPARK_HOME="${HOME}/spark/spark-3.5.5-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

```bash
spark-shell

val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```

**Paso 3: configurar PySpark**

```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

```bash
cd 
cd data-engineering-zoomcamp/w5
```

```bash
mkdir tmp
cd tmp
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

```bash
pipenv shell
jupyter notebook &
```

**Persistir las variables de entorno (opcional)**

Para evitar hacer los export cada vez que inicia una sesion por consola puede hacer lo siguiente:

```bash
nano ~/.bashrc
```

Pegar todo los export necesarios y guardar (puede ser necesario reemplazar ${HOME} por una ubicación en duro)

```bash
export JAVA_HOME="${HOME}/java11/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="${HOME}/spark/spark-3.5.5-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

## 3. Usando PySpark

Import necesarios
```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
```


Crear una sesión de Spark en Python, usando [SparkSession](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html), que es la entrada principal para trabajar con DataFrames en PySpark.

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()
```


Leer un archivo [CSV](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) y crear un DataFrame de Spark.

```python
df = spark.read \
    .option("header", "true") \
    .csv('../tmp/taxi_zone_lookup.csv')
```


[Mostrar](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.show.html) en pantalla las primeras 20 filas del DataFrame de PySpark, en formato de tabla

```python
df.show()
```


[Guardar](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.parquet.html) el DataFrame en formato Parquet dentro de una carpeta

```python
df.write.parquet('../tmp/parquet/zones', mode="overwrite")
```


Devolver el [esquema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.schema.html) del DataFrame como un objeto StructType de PySpark

```python
df.schema
```


Imprimir el [esquema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.printSchema.html) del DataFrame de forma legible en la consola

```python
df.printSchema()
```


Convertir un DataFrame de pandas en un [DataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html) de PySpark.

```python
spark.createDataFrame(df_pandas)
```


Definir de forma explícita la estructura ([schema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html)) de un DataFrame de PySpark.

```python
schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True)
])
```


Repartir ([divide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)) el DataFrame df en N particiones.

```python
df = df.repartition(24)
```


Convertir una función normal de Python en una [UDF](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html) (User Defined Function) para usar dentro de un DataFrame de Spark.

```python
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
```


[Crear](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html) N nuevas columnas en un DataFrame de Spark aplicando funciones predefinidas y UDF a ciertas columnas. Por ultimo, [seleccionar](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html) ciertas columnas.

```python
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```


Aplicar [filtros](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html) a un DataFrame de Spark.

```python
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
  .filter(df.hvfhs_license_num == 'HV0003') \
  .show()
```


[Renombra](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnRenamed.html) una columna de un DataFrame de Spark.

```python
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
```


[Une](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html) dos DataFrames con las mismas columnas

```python
df_trips_data = df_green_sel.union(df_yellow_sel)
```


[Agrupa](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html) los datos según una o más columna de un DataFrame de Spark

```python
df_trips_data.groupBy('service_type').count().show()
```


[Crea](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html) una vista temporal en Spark SQL a partir de un DataFrame de Spark

```python
df_trips_data.createOrReplaceTempView ('trips_data')
```

Ejecuta una sentencia SQL sobre la vista temporal creada

```python
spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()
```


[Reduce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html) el número de particiones del DataFrame

```python
df_result.coalesce(1).write.parquet('../tmp/data/report/revenue/', mode='overwrite')
```


[Une](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html) dos DataFrame de Spark

```python
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
```

[Convierte](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.rdd.html) un DataFrama en un RDD

```python
rdd = df_green \
    .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
    .rdd
```


[Filtra](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.filter.html) las filas del RDD, [transforma](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.map.html) cada fila en una tupla clave-valor y [agrupa por la clave](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html) generada y sumar los valores o realiza otro tipo de agregación.

```python
df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue) \
    .take(5)
```


[Convertir](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toDF.html) a un DataFrama de Spark.

```python
df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue) \
    .map(unwrap) \
    .toDF(result_schema)
```



[Operación por particiones](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.mapPartitions.html) completas (no por filas), lo que maximiza la eficiencia cuando quieres aplicar un modelo pesado o de Machine Learning, ya que puedes procesar varias filas en lote

```python
df_predicts = duration_rdd \
    .mapPartitions(apply_model_in_batch)\
    .toDF() \
    .drop('Index')
```

## 4. Descarga de archivos usando script bash

Carpeta de scripts
```bash
mkdir scripts
```

```bash
cd scripts
touch download_data.sh
```

Contenido del script
```bash
set -e

TAXI_TYPE=$1
YEAR=$2

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="../tmp/data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

done
```
```bash
chmod +x download_data.sh
./download_data.sh green 2020
./download_data.sh green 2021
./download_data.sh yellow 2020
./download_data.sh yellow 2021
```

## 5. Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- *[Procesamiento por lotes](https://aws.amazon.com/es/what-is/batch-processing/)*
- *[Spark](https://spark.apache.org/)*
- *[Quick start](https://spark.apache.org/docs/latest/quick-start.html)*
- *[Examples](https://github.com/apache/spark/tree/master/examples/src/main/python)*
- *[Data Types](https://spark.apache.org/docs/3.5.2/sql-ref-datatypes.html)*
- *[Transformation and action](https://medium.com/codex/spark-transformation-and-action-a-deep-dive-f351bce88086)*
- *[PySpark - overview](https://spark.apache.org/docs/latest/api/python/index.html)*
- *[PySpark - getting started](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)*
- *[PySpark - user guide](https://spark.apache.org/docs/latest/api/python/user_guide/index.html)*
- *[PySpark - reference](https://spark.apache.org/docs/latest/api/python/reference/index.html)*















