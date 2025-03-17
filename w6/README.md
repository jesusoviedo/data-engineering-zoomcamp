# Semana 6

## 1. Comandos básicos

Aprende los [comandos](../docs/linux_commands.md) clave que te ayudarán a realizar las tareas más comunes de manera rápida y eficiente.

## 2. Entornos de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install kafka-python
```


## 3. Utilizando redpanda, kafka, pyflink y postgresql

Descargar Makefile
```bash
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/06-streaming/pyflink/Makefile
```

Descargar Dockerfile.flink
```bash
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/06-streaming/pyflink/Dockerfile.flink
```


Descargar docker-compose.yml
```bash
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/06-streaming/pyflink/docker-compose.yml
```

Iniciar docker compose (levanta imagenes de postgresql, pgadmin, redpanda, jobmanager y taskmanager)
```bash
docker compose up
```


Creacion de tablas en PostgreSQL
```sql
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
)
```

```sql
CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP(3),
    test_data INT,
    num_hits BIGINT,
	PRIMARY KEY (event_hour, test_data)
)
```


Crear carpeta de productores
```bash
mkdir src/producers
```

Descargar producer.py
```bash
cd src/producers
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/06-streaming/pyflink/src/producers/producer.py
```

Ejecutar producer.py
```bash
pipenv shell
python src/producers/producer.py
```

Crear carpeta de job
```bash
mkdir src/job
```

Descargar start_job.py
```bash
cd src/job
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/06-streaming/pyflink/src/job/start_job.py
```

Ejecutar start_job.py
```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job.py --pyFiles /opt/src -d
```


Descargar aggregation_job.py
```bash
cd src/job
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/06-streaming/pyflink/src/job/aggregation_job.py
```

Ejecutar start_job.py
```bash
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregation_job.py --pyFiles /opt/src -d
```


## 5. Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*















