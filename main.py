import os
import shutil
from public.connections.runFrontendReact import RunFrontendReact
from src.app.scripts.csvManager import getPathCSV, getCSVData, getCSVSample, uploadCSVToCassandra, createCleanCSV, transformUploadData
from src.app.scripts.postProcessing import clearBuffer, cleanTemporaryFiles
from src.models.collectionsStructure import CollectionsStructureModel
from src.app.scripts.debugCSV import debug
from colorama import init, Fore, Style
import asyncio
import src.server as server
import src.config as config
from dotenv import load_dotenv
import time
import sys


# Obtenemos los datos de configuración del archivo config.py
URL = config.url
keyspace = config.keyspace
typeData = config.typeData
tables = config.familyColumns

# Se cargan las variables de entorno desde el archivo .env
load_dotenv()

# Animacion de carga del backend
def animacion_de_carga(total):
    try:
        print('Cargando', end=' ')
        for i in range(total):
            print(Fore.GREEN + '█', end='')
            time.sleep(0.05)
            sys.stdout.flush()
        print('\nCarga completada✅')
    except KeyboardInterrupt:
        sys.exit()
        
# Inicia el frontend de React
def run_frontend():
    print(Fore.BLUE + Style.BRIGHT +">>_Frontend React🚀")
    frontend = RunFrontendReact()
    # Obtén la ruta absoluta de la carpeta frontend
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(backend_dir)
    frontend_path = os.path.join(project_root, 'frontend')
    frontend.path = frontend_path
    frontend.run()

# Sube los datos del CSV a la base de datos Cassandra   
async def selectCassandra(debugData, servertype):
    # Inicia el servidor WebSocket
    server_instance, cluster, session = await server.server(debugData, servertype)  # Espera a que la corutina server() se complete y devuelve el objeto servidor
    message = server.check_server(server_instance)
    print(Fore.BLUE+ message)
    # Sube los datos del CSV a la base de datos Cassandra. Nota: esto es provisional
    message = uploadCSVToCassandra(keyspace, tables, typeData, debugData, session)
    print(Fore.WHITE, message)
    return server_instance

# Sube los datos del CSV al cluster de mongoDB atlas
async def selectMongoDB(debugData, servertype):
    server_instance = await server.server(debugData, servertype)  # Espera a que la corutina start_server_mongo() se complete y devuelve el objeto servidor
    # Si el server instance es una tupla de dos elementos
    if isinstance(server_instance, tuple) and len(server_instance) == 2:
        message = server.check_server(server_instance[0])
        print(Fore.GREEN + message)
    # Almacena el servidor y el cliente en variables separadas
    client = server_instance[1]  # El segundo elemento de la tupla es el cliente
    collectionStructure=CollectionsStructureModel()
    collectionStructure.load()
    # Obtener las estructuras cargadas
    structures = collectionStructure.get_structures()
    # Transforma los datos del CSV a un formato JSON
    message = transformUploadData(debugData, structures, client)
    print(Fore.WHITE + message)
    return server_instance

# # Elimina el directorio __pycache__
def remove_pycache():
    pycache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '__pycache__')
    if os.path.exists(pycache_dir):
        shutil.rmtree(pycache_dir)
        print(f"Eliminado {pycache_dir}")
    else:
        print("No se encontró __pycache__")

# Función principal del backend
async def main():
    servertype = os.getenv('SERVER_TYPE')
    # Inicia el backend
    print(Fore.BLUE + Style.BRIGHT +">>_Backend " + servertype + "-Websocket🛢️")
    animacion_de_carga(100)
    # Tratamineto de los datos del CSV
    message, path = getPathCSV()
    if path is not None:
        print(Fore.WHITE + message)
        # Se obtienen los datos del CSV
        # message, data, warningsList   = getCSVData(path)
        message, data = getCSVSample()
        print(Fore.WHITE + Style.BRIGHT + message)
        if data is not None:
            # Se le hace una depuracion a los datos del CSV
            debugData, message = debug(data, path)
            if debugData is not None:
                print(Fore.WHITE + message)
                # Se crea un nuevo CSV con los datos depurados
                message = createCleanCSV(debugData, path)
                print(Fore.WHITE + message)
                # Se limpian los archivos temporales
                clearBuffer()
                cleanTemporaryFiles()
                # Logica para subir los datos a la base de datos seleccionada
                if servertype == 'Cassandra':
                    server_instance = await selectCassandra(debugData, servertype)
                elif servertype == 'MongoDB':
                    server_instance = await selectMongoDB(debugData, servertype)
                server = server_instance[0]
                if server is not None:
                    print(Fore.BLUE + Style.BRIGHT +"Servidor WebSocket iniciado en ws://localhost:8765")
                # Espera tanto al servidor WebSocket como a otras tareas
                await asyncio.gather(
                    server.wait_closed(),  # Espera a que el servidor WebSocket se cierre
                )
            else:
                message = "No se pudo depurar los datos del CSV🚫"
                print(Fore.RED + Style.BRIGHT + message)
        else:
            message = "No se pudo cargar el archivo CSV🚫"
            print(Fore.RED + Style.BRIGHT + message)
    else:
        message = "No se pudo obtener la ruta del archivo CSV🚫"
        print(Fore.RED + Style.BRIGHT + message)
# Se inicializa colorama
init()
# Llamamos a asyncio.run(main()) para iniciar la tarea principal utilizando asyncio.run
asyncio.run(main())