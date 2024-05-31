# eliminar datos a una tabla de Cassandra.
def deleteData(session, keyspace, table):
    """
    Elimina todos los datos de una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.
    """
    # Crear la query para eliminar los datos
    query = f"TRUNCATE {keyspace}.{table};"
    # Eliminar los datos de la tabla
    session.execute(query)
    print(f"Datos eliminados de la tabla {table} de Cassandra.")