def checkTableOutline(session, keyspace, tableName):
    # Cambiar al keyspace
    session.set_keyspace(keyspace)
    # Consultar la definición de la tabla
    query = f"SELECT * FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{tableName}'"
    result = session.execute(query)
    # Mostrar la definición de la tabla
    for row in result:
        print(row)

def checkColumnOutline(session, tableName, keyspace):
    # Cambiar al keyspace
    session.set_keyspace(keyspace)
    # Consultar todas las columnas de la tabla
    query = f"SELECT * FROM system_schema.columns WHERE keyspace_name = '{keyspace}' AND table_name = '{tableName}'"
    result = session.execute(query)
    if result:
        # Obtener los nombres de las columnas iterando sobre el resultado
        tableColumns = [row.column_name for row in result]
        message = f"Las columnas de la tabla {keyspace}.{tableName} son: {tableColumns} 📅"
    else:
        tableColumns = []
        message = f"La tabla {keyspace}.{tableName} no tiene columnas"
    return tableColumns, message
        
def checkExistenceOfTables(session, table, keyspace):
        # Cambiar al keyspace
    session.set_keyspace(keyspace)
    # Consultar si la tabla existe en el keyspace
    query = f"SELECT * FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'"
    result = session.execute(query)
    if result:
        # Verificar si hay filas en el resultado para determinar la existencia de la tabla
        exists = len(list(result)) > 0
        if exists:
            message = f"La tabla {keyspace}.{table} existe ✅"
        else:
            message = f"La tabla {keyspace}.{table} no existe 🚫"
    else:
        message = f"Error al verificar la existencia de la tabla {keyspace}.{table}"
        exists = False
    return exists, message
            
                
def checkExistenceOfColumns(session, columns, keyspace):
    # Verificar si las columnas existen
     for key, values in columns.items():
        for value in values:
            # Le quitamos los corchetes y los espacios a la columna
            processed_values = value.replace("[","").replace("]","").replace(" ", "_").replace(",_", " ")
            print(f"columnas: {processed_values}")
            query = f"SELECT * FROM system_schema.columns WHERE keyspace_name = '{keyspace}' AND table_name = '{key}' AND column_name = '{processed_values}'"
            print(f"query: {query}")
            result = session.execute(query)
            if not result:
                message = f"La columna {value} en la tabla {keyspace}.{key} no existe🚫"
                return message
            else:
                message = f"La columna {value} en la tabla {keyspace}.{key} existe✅"
                return message
                    
def checkExistenceOfkeyspace(session, keyspace):
        # Verificar si el keyspace existe
        query = f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace}'"
        result = session.execute(query)
        if not result:
            message = f"El keyspace {keyspace} no existe🚫"
            return message
        else:
            message = f"El keyspace {keyspace} existe✅"
            return message