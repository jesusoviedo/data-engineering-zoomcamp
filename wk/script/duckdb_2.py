import duckdb

# Crear una conexión en memoria
db = duckdb.connect()

# Crear una tabla y consultar datos
db.execute("""
    CREATE TABLE personas (
        id INTEGER,
        nombre STRING,
        edad INTEGER,
        ciudad STRING
    )
""")

db.execute("""
    INSERT INTO personas VALUES
    (1, 'Ana', 30, 'Madrid'),
    (2, 'Luis', 25, 'Barcelona'),
    (3, 'María', 28, 'Valencia'),
    (4, 'Carlos', 35, 'Sevilla')
""")

# Consultar datos
result = db.execute("SELECT * FROM personas").fetchall()
for row in result:
    print(row)

# Realizar una consulta con filtrado
db.execute("SELECT * FROM personas WHERE edad > 28").fetchall()