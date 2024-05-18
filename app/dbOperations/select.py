# Consultar datos en Cassandra
def selectData(keyspace, familys, sessionServer):
    """
    Consulta los datos de todas las tablas de Cassandra.

    Args:
        keyspace: Keyspace de Cassandra.
        familys: Familias de columnas de Cassandra.
        sessionServer: Sesión de conexión a Cassandra.

    Returns:
        Resultado de la consulta.
    """
    try:
        print(familys)
        results = []
        familys = familys[0]
        for table in familys.values():  # Iterar sobre los valores de 'familys' en lugar de sobre las claves
            result = selecTable(sessionServer, keyspace, table)
            results.append(result)
        message = "Datos consultados en Cassandra.", results
        return message, results
    except Exception as e:
        message = f"Error al consultar los datos en Cassandra: {e}"
        return message, None
      
# consultar coincidencias en cassandra
def selectMatches(session, keyspace, table, data):
    """
    Consulta una fila en una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.
        data: Datos a consultar en la tabla.

    Returns:
        Resultado de la consulta.
    """
    # Crear la query para consultar los datos
    query = f"SELECT * FROM {keyspace}.{table} WHERE "
    for i, key in enumerate(data.keys()):
        query += f"{key} = '{data[key]}'"
        if i != len(data.keys()) - 1:
            query += " AND "
    query += ";"
    # Consultar los datos en la tabla
    result = session.execute(query)
    print(f"Datos consultados en la tabla {table} de Cassandra.")
    return result

# consultar por tabla de Cassandra
def selecTable(session, keyspace, table):
    """
    Consulta todas las filas de una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.

    Returns:
        Resultado de la consulta.
    """
    # Crear la query para consultar los datos
    query = f"SELECT * FROM {keyspace}.{table};"
    # Consultar los datos en la tabla
    result = session.execute(query)
    return result

# consultar el primer registro de una tabla de Cassandra
def selectFirstRow(session, keyspace, table):
    """
    Consulta el primer registro de una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.

    Returns:
        Resultado de la consulta.
    """
    # Crear la query para consultar los datos
    query = f"SELECT * FROM {keyspace}.{table} LIMIT 1;"
    # Consultar los datos en la tabla
    result = session.execute(query)
    print(f"Datos consultados en la tabla {table} de Cassandra.")
    return result

# Consultar por columna de una tabla de Cassandra
def selectColumn(session, keyspace, table, column):
    """
    Consulta una columna de una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.
        column: Columna de la tabla.

    Returns:
        Resultado de la consulta.
    """
    # Crear la query para consultar los datos
    query = f"SELECT {column} FROM {keyspace}.{table};"
    # Consultar los datos en la tabla
    result = session.execute(query)
    print(f"Columna {column} consultada en la tabla {table} de Cassandra.")
    return result