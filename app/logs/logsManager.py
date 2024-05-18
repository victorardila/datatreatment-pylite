import datetime
import os
import uuid
import json
from pathlib import Path

# metodo para guardar logs en un archivo de texto guardará los logs en la carpeta logs del proyecto
def saveLog(logs):
    try:
        # Se obtiene la fecha y hora actual
        now = datetime.datetime.now()
        # Se crea un uid
        uid = uuid.uuid4()
        # Obtener la ruta del archivo actual
        ruta_actual = Path(__file__).parent
        # Subir dos niveles
        ruta_dos_niveles_arriba = ruta_actual.parent.parent
        # Verificar si la carpeta existe, si no, crearla
        if not os.path.exists(ruta_dos_niveles_arriba / 'app' / 'logs' / 'txt'):
            os.makedirs(ruta_dos_niveles_arriba / 'app' / 'logs' / 'txt')
        # Se crea el nombre del archivo
        file = f'log_{uid}_{now.strftime("%d-%m-%Y_%H-%M-%S")}.txt'
        # Ruta al archivo .bat
        path_file = ruta_dos_niveles_arriba / 'app' / 'logs' / 'txt'/ file
        # Se abre el archivo en modo escritura
        with open(path_file, 'w') as file:
            # Se recorre cada registro de log
            for log in logs:
                # Se convierte el registro de log a formato JSON
                log_json = json.dumps(log)
                # Se escribe el registro en el archivo seguido por punto y coma y un salto de línea
                file.write(log_json + ';\n')
    except Exception as e:
        print(f'Error al guardar el log: {e}')
                
# metodo para leer los logs de la carpeta logs del proyecto
def readLogs():
    try:
        # Se obtiene la lista de archivos en la carpeta logs
        logs = os.listdir('./app/logs/txt')
        # Se crea una lista para guardar los logs
        logsList = []
        # Se recorre la lista de archivos
        for log in logs:
            # Se lee el archivo
            with open(f'./app/logs/txt/{log}', 'r') as file:
                # Se guarda el log en la lista
                logsList.append(file.read())
        # Se retorna la lista de logs
        return logsList
    except Exception as e:
        print(f'Error al leer los logs: {e}')
        return None

# metodo para eliminar un log de la carpeta logs del proyecto
def deleteLog(log):
    try:
        # Se elimina el archivo
        os.remove(f'./app/logs/txt/{log}')
    except Exception as e:
        print(f'Error al eliminar el log: {e}')
        
# metodo para eliminar todos los logs de la carpeta logs del proyecto
def deleteAllLogs():
    try:
        # Se obtiene la lista de archivos en la carpeta logs
        logs = os.listdir('./app/logs/txt')
        # Se recorre la lista de archivos
        for log in logs:
            # Se elimina el archivo
            os.remove(f'./app/logs/txt/{log}')
    except Exception as e:
        print(f'Error al eliminar los logs: {e}')