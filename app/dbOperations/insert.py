from cassandra.cqlengine.models import Model

# insertar datos a una tabla de Cassandra.
def insertRow(session, keyspace, table, data):
    """
    Inserta una fila en una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.
        data: Datos a insertar en la tabla.
    """
    # Crear la query para insertar los datos
    query = f"INSERT INTO {keyspace}.{table} ("
    for i, key in enumerate(data.keys()):
        query += key
        if i != len(data.keys()) - 1:
            query += ", "
    query += ") VALUES ("
    for i, value in enumerate(data.values()):
        query += f"'{value}'"
        if i != len(data.values()) - 1:
            query += ", "
    query += ");"
    # Insertar los datos en la tabla
    session.execute(query)
    print(f"Datos insertados en la tabla {table} de Cassandra.")
    