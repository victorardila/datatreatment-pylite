#Crear una tabla en Cassandra
def createTable(session, keyspace, table, headers):
  """
  Crea una tabla en Cassandra a partir de un archivo CSV.

  Args:
    session: Sesión de conexión a Cassandra.
    keyspace: Keyspace de Cassandra.
    table: Nombre de la tabla a crear.
    csv_file: Ruta al archivo CSV.
  """
  # Generar la consulta CREATE TABLE
  query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} ("
  query += "id UUID PRIMARY KEY,"

  for header in headers:
    query += f"{header} TEXT,"

  # Eliminar la última coma
  query = query[:-1]

  query += ");"

  # Ejecutar la consulta
  session.execute(query)

  print(f"Tabla {table} creada en Cassandra.")

