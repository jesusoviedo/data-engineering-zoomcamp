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
 