import duckdb
import json
import tempfile

# Definir un JSON con datos de ejemplo
data = [{'id': 1, 'nombre': 'Ana', 'edad': 30, 'ciudad': 'Madrid'},
              {'id': 2, 'nombre': 'Luis', 'edad': 25, 'ciudad': 'Barcelona'},
              {'id': 3, 'nombre': 'María', 'edad': 28, 'ciudad': 'Valencia'},
              {'id': 4, 'nombre': 'Carlos', 'edad': 35, 'ciudad': 'Sevilla'}]

data_json = json.dumps(data)
with tempfile.NamedTemporaryFile(delete=False, mode="w", newline="") as f:
    f.write(data_json)
    temp_file_path = f.name

# Convertir el JSON a una tabla en DuckDB
db = duckdb.connect()
db.execute("CREATE TABLE personas AS SELECT * FROM read_json_auto(?)", [temp_file_path])

# Consultar la tabla
result = db.execute("SELECT * FROM personas").fetchall()
for row in result:
    print(row)