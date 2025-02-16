import pyarrow as pa
import duckdb

# Crear un esquema de tabla en PyArrow
schema = pa.schema([
    ('id', pa.int32()),
    ('nombre', pa.string()),
    ('edad', pa.int32()),
    ('ciudad', pa.string())
])

# Crear los datos (como listas de Python)
data = [
    [1, 'Ana', 30, 'Madrid'],
    [2, 'Luis', 25, 'Barcelona'],
    [3, 'María', 28, 'Valencia'],
    [4, 'Carlos', 35, 'Sevilla']
]

# Convertir los datos a columnas de PyArrow
table = pa.table({field.name: pa.array([row[i] for row in data]) for i, field in enumerate(schema)})

# Crear una conexión a DuckDB
conn = duckdb.connect()

# Registrar la tabla PyArrow en DuckDB usando from_arrow
conn.register('arrow_table', table)

# Ejecutar una consulta SQL sobre la tabla registrada
result = conn.execute("SELECT * FROM arrow_table WHERE edad > 28").df()

# Mostrar el resultado
print(result)
