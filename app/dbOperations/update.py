#Actualizar todas las coincidencias de una tabla de Cassandra
def updateRow(session, keyspace, table, data, newData):
    """
    Actualiza todas las coincidencias de una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.
        data: Datos a buscar en la tabla.
        newData: Nuevos datos a actualizar en la tabla.
    """
    # Crear la query para actualizar los datos
    query = f"UPDATE {keyspace}.{table} SET "
    for i, key in enumerate(newData.keys()):
        query += f"{key} = '{newData[key]}'"
        if i != len(newData.keys()) - 1:
            query += ", "
    query += " WHERE "
    for i, key in enumerate(data.keys()):
        query += f"{key} = '{data[key]}'"
        if i != len(data.keys()) - 1:
            query += " AND "
    query += ";"
    # Actualizar los datos en la tabla
    session.execute(query)
    print(f"Datos actualizados en la tabla {table} de Cassandra.")

#Actualizar solo la primera coincidencia de una tabla de Cassandra
def updateFirstRow(session, keyspace, table, data, newData):
    """
    Actualiza solo la primera coincidencia de una tabla de Cassandra.

    Args:
        session: Sesión de conexión a Cassandra.
        keyspace: Keyspace de Cassandra.
        table: Tabla de Cassandra.
        data: Datos a buscar en la tabla.
        newData: Nuevos datos a actualizar en la tabla.
    """
    # Crear la query para actualizar los datos
    query = f"UPDATE {keyspace}.{table} SET "
    for i, key in enumerate(newData.keys()):
        query += f"{key} = '{newData[key]}'"
        if i != len(newData.keys()) - 1:
            query += ", "
    query += " WHERE "
    for i, key in enumerate(data.keys()):
        query += f"{key} = '{data[key]}'"
        if i != len(data.keys()) - 1:
            query += " AND "
    query += " LIMIT 1;"
    # Actualizar los datos en la tabla
    session.execute(query)
    print(f"Datos actualizados en la tabla {table} de Cassandra.")