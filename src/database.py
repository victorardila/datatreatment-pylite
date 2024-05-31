import os
import subprocess
import pygetwindow
import socket
import time
from pymongo import MongoClient

# Cassandra functions
def db_exists_cassandra(cluster, keyspace):
    """
    Verifica si un keyspace existe en la base de datos Cassandra.
    """
    return keyspace in cluster.metadata.keyspaces

def is_cassandra_running(host, port):
    """
    Verifica si Cassandra está corriendo en el host y puerto especificados.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host, port))
    sock.close()
    return result == 0

def close_cmd_window():
    """
    Cierra cualquier ventana de consola abierta.
    """
    for ventana in pygetwindow.getWindowsWithTitle("cmd"):
        ventana.close()

def init_cassandra():
    """
    Iniciar Cassandra y establecer la conexión con la base de datos.
    """
    print("Iniciando la base de datos Cassandra🗃️")
    max_intentos = 100
    intentos = 0
    close_cmd_window()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_bat = os.path.join(directorio_actual, "app", "bat", "startCassandra.bat")
    proceso = subprocess.Popen(ruta_bat, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    
    while not is_cassandra_running("localhost", 9042) and intentos < max_intentos:
        time.sleep(2)
        intentos += 1
    if intentos >= max_intentos:
        print("Error: No se pudo iniciar Cassandra.")
        proceso.terminate()
        return None, None
    
    cluster, session = connection.init()  # Iniciar la conexión a la base de datos
    return cluster, session

def stop_cassandra():
    """
    Detener Cassandra y cerrar la conexión con la base de datos.
    """
    close_cmd_window()

# MongoDB functions cluster
def init_mongodb_cluster(uri, dbName):
    """
    Iniciar la conexión con MongoDB
    
    Retorno:
        client: El cliente de MongoDB.
        db: La base de datos de MongoDB.
    """
    print("Iniciando la conexión con MongoDB Atlas🗄️")
    client = MongoClient(uri)
    db = client[dbName]
    # obtengo todas las colecciones si existen
    collectionsObtained = db.list_collection_names()
    return client, db, collectionsObtained

def is_mongodb_cluster_running(uri):
    """
    Verifica si MongoDB Atlas está corriendo.
    """
    try:
        client = MongoClient(uri)
        client.server_info()
        return True
    except Exception as e:
        return False

def close_cluster_mongodb(client):
    """
    Cerrar la conexión con MongoDB Atlas.
    """
    client.close()

# MongoDB functions local
def init_mongodb_local():
    """
    Iniciar la conexión con MongoDB local.
    
    Retorno:
        client: El cliente de MongoDB.
        db: La base de datos de MongoDB.
    """
    print("Iniciando la conexión con MongoDB local🗄️")
    client = MongoClient("mongodb://localhost:27017/")
    db = client["local"]
    # obtengo todas las colecciones si existen
    collectionsObtained = db.list_collection_names()
    return client, db, collectionsObtained

def is_mongodb_local_running():
    """
    Verifica si MongoDB local está corriendo.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        client.server_info()
        return True
    except Exception as e:
        return False

def close_mongodb_local(client):
    """
    Cerrar la conexión con MongoDB local.
    """
    client.close()
    
