import websockets
import sys
from database import init, stop
from app.scripts.csvManager import uploadCSVToCassandra
from app.dbOperations.select import selectData
import config as config

URL = config.url
keyspace = config.keyspace
typeData = config.typeData
tables = config.familyColumns

async def websocket_server(websocket, path):
    try:
        async for message in websocket:
            if path == '/uploadcsv':  # Ruta para subir CSV a la base de datos
                message, results=uploadCSVToCassandra(keyspace, tables, dataServer, sessionServer)  # Llama a la función para cargar el CSV
                print(message)
                await websocket.send(str(results))  # Envía el resultado al cliente
            elif path == '/':  # Ruta para consultar datos de Cassandra
                result = selectData(keyspace, tables, sessionServer)
                await websocket.send(str(result))  # Envía el resultado al cliente
            # elif path == '/get_keyspaces':  # Ruta para obtener los keyspaces creados en Cassandra
            #     keyspaces = get_keyspaces()
            #     await websocket.send(str(keyspaces))  # Envía los keyspaces al cliente

            # elif path.startswith('/get_columns'):  # Ruta para obtener las columnas de una tabla
            #     keyspace, table = path.split('/')[2:]  # Obtiene el keyspace y la tabla de la ruta
            #     columns = get_columns(keyspace, table)
            #     await websocket.send(str(columns))  # Envía las columnas al cliente

    except Exception as e:
        print(f"Error en websocket_server: {e}")

    finally:
        await websocket.close()# verificar si el servidor se ha iniciado correctamente

def check_server(server_instance):
    if server_instance.is_serving():
        message = "Servidor WebSocket iniciado en localhost:8765🌍"
        # Borrar la salida anterior
        sys.stdout.write('\r' + ' ' * len("Procesando...") + '\r')
        sys.stdout.flush()
        return message
    else:
        message="Error al iniciar el servidor WebSocket"
        # Borrar la salida anterior
        sys.stdout.write('\r' + ' ' * len("Procesando...") + '\r')
        sys.stdout.flush()
        return message

async def server(data):
    global clusterServer 
    global sessionServer
    global dataServer
    # Inicia la conexión con cassandra
    cluster, session = init()
    clusterServer=cluster
    sessionServer=session
    dataServer=data
    # Inicia el servidor WebSocket
    server = await websockets.serve(websocket_server, 'localhost', 8765)
    return server, cluster, session
    
