import os
import subprocess
import pygetwindow
import connection
import socket
import time

def db_exists(cluster, keyspace):
    """
    Verifica si un keyspace existe en la base de datos.
    
    Args:
        cluster: El clúster de Cassandra.
        keyspace: El keyspace a verificar.
    
    Retorno:
        True si el keyspace existe, False en caso contrario.
    """
    return keyspace in cluster.metadata.keyspaces

def is_cassandra_running(host, port):
    """
    Verifica si Cassandra está corriendo en el host y puerto especificados.
    
    Args:
        host: El host donde se encuentra Cassandra.
        port: El puerto en el que Cassandra está escuchando.
    
    Retorno:
        True si Cassandra está corriendo, False en caso contrario.
    """
    # Intenta establecer una conexión TCP con el host y puerto de Cassandra
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
        
def init():
    """
    Iniciar Cassandra y establecer la conexión con la base de datos.
    
    Retorno:
        cluster: El clúster de Cassandra.
        session: La sesión de conexión con la base de datos.
    """
    print("Iniciando la base de datos Cassandra🗃️")
    # Intentos máximos para iniciar Cassandra
    max_intentos = 100
    intentos = 0
    # Cerrar la ventana cmd si está abierta
    close_cmd_window()
    # Obtener la ruta del archivo .bat para iniciar Cassandra
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_bat = os.path.join(directorio_actual, "app", "bat", "startCassandra.bat")
    # Iniciar Cassandra
    proceso=subprocess.Popen(ruta_bat, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    # Esperar hasta que Cassandra esté completamente iniciado o hasta que pase un tiempo máximo
    while not is_cassandra_running("localhost", 9042) and intentos < max_intentos:
        time.sleep(2)  # Esperar 2 segundos antes de cada intento
        intentos += 1
    if intentos >= max_intentos:
        print("Error: No se pudo iniciar Cassandra.")
        proceso.terminate()  # Terminar el proceso de Cassandra si no se inició correctamente
        return
    cluster, session = connection.init()  # Iniciar la conexión a la base de datos
    return cluster, session

def stop():
    """
    Detener Cassandra y cerrar la conexión con la base de datos.
    """
    close_cmd_window()
