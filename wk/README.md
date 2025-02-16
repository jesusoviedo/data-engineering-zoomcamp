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

## 3. DuckDB

**Usando DuckDB**

Ejemplo simpple
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







###  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- *[Python API - DuckDB](https://duckdb.org/docs/clients/python/overview.html)*
- *[Documentación - DuckDB](https://duckdb.org/docs/index)*
- *[GitHub - DuckDB](https://github.com/duckdb/duckdb)*










