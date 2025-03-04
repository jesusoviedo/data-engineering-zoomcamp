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


##  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- *[Procesamiento por lotes](https://aws.amazon.com/es/what-is/batch-processing/)*
- *[Spark](https://spark.apache.org/)*



[Quick start](https://spark.apache.org/docs/latest/quick-start.html)
[Examples](https://github.com/apache/spark/tree/master/examples/src/main/python)

[text](https://spark.apache.org/docs/latest/api/python/index.html)
[text](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)
[text](https://spark.apache.org/docs/latest/api/python/user_guide/index.html)
[text](https://spark.apache.org/docs/latest/api/python/reference/index.html)


https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html














