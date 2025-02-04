## Home Work week 2


### 1. Entorno de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install --dev pandas pgcli notebook
```

Carpeta de notebooks

```bash
mkdir notebooks
```

### 2. Iniciar notebook

Iniciar notebook

```bash
pipenv shell
```

```bash
jupyter notebook &
```

### 3. PostgreSQL + PgAdmin

Carpeta para docker-compose de PostgreSQL + PgAdmin

```bash
mkdir postgres
cd postgres
```

*[Docker Compose](./postgres/docker-compose.yaml)* utilizado para tener una base de PostgreSQL y PgAdmin

```yaml
  services:
    postgres:
      image: postgres
      container_name: postgres-db
      environment:
        POSTGRES_USER: kestr5-user
        POSTGRES_PASSWORD: k3sTR5
        POSTGRES_DB: postgres-zoomcamp-w2
      ports:
        - "5432:5432"
      volumes:
        - postgres-data:/var/lib/postgresql/data
    postgres-ui:
      image: dpage/pgadmin4
      environment:
        - PGADMIN_DEFAULT_EMAIL=w2@dataengi.com
        - PGADMIN_DEFAULT_PASSWORD=w2dataengi
      ports:
        - 8082:80
      volumes:
        - pgadmin-data:/var/lib/pgadmin
  volumes:
    postgres-data:
    pgadmin-data:
```

Iniciar PostgreSQL y PgAdmin

```bash
docker compose up -d
```

### 4. Kestra

Carpeta para docker-compose de Kestra

```bash
mkdir kestra
cd kestra
```

*[Docker Compose](./kestra/docker-compose.yaml)* utilizado para tener Kestra

```yaml
volumes:
  postgres-data:
    driver: local
  kestra-data:
    driver: local

services:
  postgres:
    image: postgres
    volumes:
      - postgres-data:/var/lib/postgresql/data
    environment:
      POSTGRES_DB: kestra
      POSTGRES_USER: kestra
      POSTGRES_PASSWORD: k3str4
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -d $${POSTGRES_DB} -U $${POSTGRES_USER}"]
      interval: 30s
      timeout: 10s
      retries: 10

  kestra:
    image: kestra/kestra:latest
    pull_policy: always
    user: "root"
    command: server standalone
    volumes:
      - kestra-data:/app/storage
      - /var/run/docker.sock:/var/run/docker.sock
      - /tmp/kestra-wd:/tmp/kestra-wd
    environment:
      KESTRA_CONFIGURATION: |
        datasources:
          postgres:
            url: jdbc:postgresql://postgres:5432/kestra
            driverClassName: org.postgresql.Driver
            username: kestra
            password: k3str4
        kestra:
          server:
            basicAuth:
              enabled: false
              username: "admin@kestra.io"
              password: kestra
          repository:
            type: postgres
          storage:
            type: local
            local:
              basePath: "/app/storage"
          queue:
            type: postgres
          tasks:
            tmpDir:
              path: /tmp/kestra-wd/tmp
          url: http://localhost:8080/
    ports:
      - "8080:8080"
      - "8081:8081"
    depends_on:
      postgres:
        condition: service_started
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

Iniciar Kestra

```bash
docker compose up -d
```

### 5. Flows kestra

Carpeta para flows de Kestra

```bash
mkdir flows
```

### 6. Desarrollo de la tarea
Descarga el archivo **[Homework2.ipynb](./notebooks/Homework2.ipynb)** en la carpeta de notebooks para revisar los detalles de la solución de la tarea