## Homework workshop


### 1. Entorno de desarrollo

Crear entorno e instalar dependencias

```bash
pipenv --python 3.12
```

```bash
pipenv install --dev notebook pandas dlt[duckdb]
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


### 3. Desarrollo de la tarea
Descarga el archivo **[HomeworkWorkshop.ipynb](./notebooks/HomeworkWorkshop.ipynb)** en la carpeta de notebooks para revisar los detalles de la solución de la tarea

*Comando para detener todos los servicios de notebook:*

```bash
pkill -f jupyter
``