from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from config import host, port
import socket

def getDataFromServer():
    """
    Obtener datos del servidor.
    """
    # Obtener la dirección IP del servidor
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname_ex(hostname)
    # Obtener el puerto del servidor cassandra sea el que tenga por defecto o el que se haya configurado
    port = IPAddr[2][0]
    # obtener nombre del servidor
    serverName = IPAddr[0]
    return IPAddr, serverName, port

def init():
    """
    Inicializar la conexión con la base de datos Cassandra.
    """
    # Definir las credenciales de autenticación
    CASSANDRA_HOST = host
    CASSANDRA_PORT = port
    cluster = None
    session = None  # Define la variable session fuera del bloque try-except
    # Intentar establecer conexión con el clúster de Cassandra
    try:
        cluster = Cluster(
            [CASSANDRA_HOST],
            port=CASSANDRA_PORT,
        )
        session = cluster.connect()
        print("Conexión exitosa con la base de datos Cassandra✅")
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir durante la conexión
        print("Error al conectar con la base de datos Cassandra:", e)
    return cluster, session  # Devuelve cluster y session después del bloque try-except

def stop():
    """
    Detener la conexión con la base de datos Cassandra.
    """
    # Cerrar la conexión con el clúster de Cassandra
    try:
        Cluster.shutdown()
        print("Conexión cerrada con la base de datos Cassandra.")
    except Exception as e:
        # Manejar cualquier excepción que pueda ocurrir al cerrar la conexión
        print("Error al cerrar la conexión con la base de datos Cassandra:", e)

# --------------------------------- Ejemplo de uso ---------------------------------
# CASSANDRA_HOST = host
# CASSANDRA_PORT = port
# CASSANDRA_USERNAME = "cassandra"
# CASSANDRA_PASSWORD = "cassandra"
# auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)
# cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
# session = cluster.connect()
# session.execute("CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
# session.set_keyspace("my_keyspace")
# session.execute("INSERT INTO my_table (id, name) VALUES (%s, %s)", (uuid4(), "John Doe"))
# rows = session.execute("SELECT * FROM my_table")
# for row in rows:
#     print(row)
# session.shutdown()
# cluster.shutdown()