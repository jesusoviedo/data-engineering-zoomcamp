## Semana 1

### 1. Comandos básicos

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


### 2. Entornos de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
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

### 3. Kestra

Se descarga la versión oficial del docker compose para iniciar kestra localmente

```bash
curl -o docker-compose.yml \
https://raw.githubusercontent.com/kestra-io/kestra/develop/docker-compose.yml
```

```bash
docker-compose up -d
```

Carpeta para configuración de postgre

```bash
mkdir postgres
cd postgres
```

Descargar archivo

```bash
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/refs/heads/main/02-workflow-orchestration/postgres/docker-compose.yml
```


Carpeta para guardar los flows de Kestra

```bash
mkdir flows
```





####  Fuentes y Documentación

Si deseas profundizar más, consulta estos recursos:

- *[Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)*
- *[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
- *[kestra docs](https://kestra.io/docs)*
- *[docker install](https://kestra.io/docs/installation/docker)*
- *[docker-compose install](https://kestra.io/docs/installation/docker-compose)*
- *[kestra quickstart](https://kestra.io/docs/getting-started/quickstart)*
- *[kestra tutorial](https://kestra.io/docs/getting-started/tutorial)*
- *[kestra concepts](https://kestra.io/docs/concepts)*
- *[kestra fundamentals](https://kestra.io/docs/tutorial/fundamentals)*
- *[kestra scripts](https://kestra.io/docs/workflow-components/tasks/scripts)*
- *[kestra inputs](https://kestra.io/docs/tutorial/inputs)*
- *[kestra workflow-components inputs](https://kestra.io/docs/workflow-components/inputs)*
- *[kestra outputs](https://kestra.io/docs/tutorial/outputs)*
- *[kestra workflow-components outputs](https://kestra.io/docs/workflow-components/outputs)*
- *[kestra triggers](https://kestra.io/docs/tutorial/triggers)*
- *[kestra workflow-components triggers](https://kestra.io/docs/workflow-components/triggers)*
- *[kestra flowable](https://kestra.io/docs/tutorial/flowable)*
- *[kestra flowable-tasks](https://kestra.io/docs/workflow-components/tasks/flowable-tasks)*
- *[kestra secret](https://kestra.io/docs/concepts/secret)*
- *[kestra how-to-guides secret](https://kestra.io/docs/how-to-guides/secrets)*
- *[kestra errors](https://kestra.io/docs/tutorial/errors)*
- *[kestra workflow-components errors](https://kestra.io/docs/workflow-components/errors)*










