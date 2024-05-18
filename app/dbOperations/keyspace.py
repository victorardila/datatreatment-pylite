# crear un keyspace en Cassandra.
def createKeyspace(session, keyspace):
    """
    Crea un keyspace en Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Nombre del keyspace a crear.
    """
    # Crear el keyspace
    query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};"
    session.execute(query)
    print(f"Keyspace {keyspace} creado en Cassandra.")