from src.app.scripts.csvManager import getPathCSV, getCSVData, uploadCSVToCassandra, createCleanCSV
from src.app.scripts.postProcessing import clearBuffer, cleanTemporaryFiles
from src.app.scripts.debugCSV import debug
from colorama import init, Fore, Style
import asyncio
import backend.src.server as server
import backend.src.config as config
import time
import sys

# Obtenemos los datos de configuración del archivo config.py
URL = config.url
keyspace = config.keyspace
typeData = config.typeData
tables = config.familyColumns

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
# Función principal del backend
async def main():
    # Mensaje de inicio
    print(Fore.BLUE + Style.BRIGHT +">>_Backend Cassandra-Websocket🛢️")
    animacion_de_carga(100)
    # Tratamineto de los datos del CSV
    message, path = getPathCSV()
    if path is not None:
        print(Fore.WHITE + message)
        # Se obtienen los datos del CSV
        message, data, warningsList   = getCSVData(path)
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
                # Inicia el servidor WebSocket
                server_instance, cluster, session = await server.server(debugData)  # Espera a que la corutina server() se complete y devuelve el objeto servidor
                message = server.check_server(server_instance)
                print(Fore.YELLOW + message)
                # Sube los datos del CSV a la base de datos Cassandra. Nota: esto es provisional
                message = uploadCSVToCassandra(keyspace, tables, typeData, debugData, session)
                print(Fore.WHITE + message)
                # Espera tanto al servidor WebSocket como a otras tareas
                await asyncio.gather(
                    server_instance.wait_closed(),  # Espera a que el servidor WebSocket se cierre
                    # Agrega otras tareas si es necesario
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