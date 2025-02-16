import pandas as pd
import duckdb

# Crear un DataFrame
data = {
    'id': [1, 2, 3, 4],
    'nombre': ['Ana', 'Luis', 'María', 'Carlos'],
    'edad': [30, 25, 28, 35],
    'ciudad': ['Madrid', 'Barcelona', 'Valencia', 'Sevilla']
}
df = pd.DataFrame(data)

# Ejecutar consultas SQL sobre el DataFrame
result = duckdb.query("SELECT * FROM df WHERE edad > 28").df()
print(result)