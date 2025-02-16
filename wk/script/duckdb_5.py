import polars as pl
import duckdb

# Crear un DataFrame de Polars
data = {
    'id': [1, 2, 3, 4],
    'nombre': ['Ana', 'Luis', 'María', 'Carlos'],
    'edad': [30, 25, 28, 35],
    'ciudad': ['Madrid', 'Barcelona', 'Valencia', 'Sevilla']
}
df_polars = pl.DataFrame(data)

# Ejecutar una consulta SQL sobre el DataFrame de Polars
result = duckdb.query("SELECT * FROM df_polars WHERE edad > 28").df()

# Mostrar el resultado
print(result)