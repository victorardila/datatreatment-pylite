import os
import shutil
import websockets
import sys
from src.database import init_cassandra, stop_cassandra
from src.app.scripts.csvManager import uploadCSVToCassandra
from src.app.dbOperations.cassandra.select import selectData
import src.config as config
from pymongo import MongoClient

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

async def start_server_mongo():
    # Conexión a la base de datos
    uriservermongo = os.getenv('URI_SERVER_MONGO')
    client = MongoClient(uriservermongo)
    
    # Inicia el servidor WebSocket
    server = await websockets.serve(websocket_server, "localhost", 8765)
    return server, client

async def start_server_cassandra(data):
    global clusterServer 
    global sessionServer
    global dataServer
    # Inicia la conexión con cassandra
    cluster, session = init_cassandra()
    clusterServer=cluster
    sessionServer=session
    dataServer=data
    # Inicia el servidor WebSocket
    server = await websockets.serve(websocket_server, 'localhost', 8765)
    return server, cluster, session

def remove_pycache():
    pycache_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "__pycache__"
    )
    if os.path.exists(pycache_dir):
        shutil.rmtree(pycache_dir)
        print(f"Eliminado {pycache_dir}")
    else:
        print("No se encontró __pycache__")

async def server(data, servertype):
    # condicional ternario para seleccionar el servidor
    server_instance = await (start_server_cassandra(data) if servertype == 'Cassandra' else start_server_mongo())
    return server_instance