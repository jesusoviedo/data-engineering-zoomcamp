# Semana 2

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
pipenv install pandas
```

```bash
pipenv install --dev pgcli
```

```bash
pipenv install --dev notebook
```

## 3. Kestra

Se descarga la versión oficial del docker compose para iniciar kestra localmente

```bash
curl -o docker-compose.yml \
https://raw.githubusercontent.com/kestra-io/kestra/develop/docker-compose.yml
```

Fix: Agregar extra_hosts para host.docker.internal en Linux

Se ha corregido la configuración de Docker Compose para solucionar el error de resolución del alias `host.docker.internal` en entornos Linux. Dado que este alias no se resuelve de forma nativa en Linux, se agregó la siguiente línea en el contenedor correspondiente:

  extra_hosts:
    - "host.docker.internal:host-gateway"

Con este cambio, los contenedores que necesiten acceder a servicios en el host mediante `host.docker.internal` podrán hacerlo correctamente. Si se ejecutan conexiones entre contenedores en la misma red, se recomienda utilizar el nombre del servicio directamente.

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

## 4. Flows en kestra


### **Diseño de flujos de trabajo con recursos locales**

Creación de flujos de trabajo que operan exclusivamente con recursos locales. Se abordan soluciones que van desde procesos sencillos hasta implementaciones complejas, integrando triggers y la ejecución de DBT para optimizar la automatización y la eficiencia.

1. *[getting_started_data_pipeline](/w2/flows/getting_started_data_pipeline.yaml)*
2. *[postgres_taxi](/w2/flows/postgres_taxi.yaml)*
3. *[postgres_taxi_scheduled](/w2/flows/postgres_taxi_scheduled.yaml)*
4. *[postgres_dbt](/w2/flows/postgres_dbt.yaml)*

### **Diseño de Flujos de Trabajo en Google Cloud Platform**

Para interactuar con Google Cloud Platform (GCP) desde Kestra y poder crear un bucket en Cloud Storage y un dataset en BigQuery, es necesario configurar una cuenta de servicio con los permisos adecuados.

**Pasos para configurar la cuenta de servicio en GCP:**
1. Crear una cuenta de servicio
- Ve a la consola de GCP: Google Cloud Console
- Navega a IAM y administración → Cuentas de servicio
- Crea una nueva cuenta de servicio y asígnale un nombre relevante.

2. Asignar permisos
- Otorga los siguientes roles a la cuenta de servicio:
- Storage Admin → Para crear y administrar buckets en Cloud Storage.
- BigQuery Admin → Para crear y administrar datasets en BigQuery.

3. Generar y descargar la clave JSON
- Una vez creada la cuenta de servicio, ve a la pestaña Claves.
- Haz clic en Agregar clave → Crear una clave nueva.
- Selecciona el formato JSON y descárgalo.
 Guarda este archivo en un lugar seguro, ya que contiene credenciales sensibles.

4. Agregar las credenciales a Kestra
- Ve a Kestra UI → Secrets (KV Store).
- Agrega un nuevo secreto con:
  - Key: GCP_CREDS
  - Value: Contenido del archivo JSON descargado.

Con esta configuración, podrás utilizar GCP_CREDS en tus flujos de Kestra para autenticarte en GCP y ejecutar tareas relacionadas con Cloud Storage y BigQuery:

1. *[gcp_kv](/w2/flows/gcp_kv.yaml)*
2. *[gcp_setup.yaml](/w2/flows/gcp_setup.yaml)*
3. *[.yaml](/w2/flows/getting_started_data_pipeline.yaml)*
4. *[.yaml](/w2/flows/getting_started_data_pipeline.yaml)*
5. *[.yaml](/w2/flows/getting_started_data_pipeline.yaml)*



###  Fuentes y Documentación

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
- *[kestra docker](https://kestra.io/docs/tutorial/docker)*
- *[kestra scripts](https://kestra.io/docs/workflow-components/tasks/scripts)*









