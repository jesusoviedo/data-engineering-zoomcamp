## Semana 1

### 1. Docker

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

Levantar PostgresSQL usando una imagen de Docker

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

En PostgreSQL, puedes usar el carácter %40 para escapar @ en una cadena de conexión

```bash
pgcli postgres://w1dataengi:w1d%40taengi@localhost:5432/ny_taxi
```


####  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- *[pipenv](https://pipenv-es.readthedocs.io/es/latest/)*
- *[pgcli](https://www.pgcli.com/)*
- *[jupyter notebook](https://jupyter.org/install)*
- *[pandas](https://pandas.pydata.org/)*
- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*



