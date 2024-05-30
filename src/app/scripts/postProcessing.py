import gc
import os
import psutil
import shutil

def clearBuffer():
    """
    Limpia el buffer de memoria.
    """
    gc.collect()
    print("Buffer limpiado✅")

def cleanTemporaryFiles():
    """
    Limpia los archivos temporales en el directorio Temp del usuario actual.
    """
    # Lista para almacenar las excepciones encontradas
    exceptions = []
    try:
        # Obtener la ruta al directorio AppData del usuario actual en Windows
        appdata_path = os.getenv('APPDATA')
        if appdata_path:
            # Eliminar 'Roaming' de la ruta y agregar 'Local\Temp'
            temp_path = os.path.join(appdata_path.replace('Roaming', 'Local'), 'Temp')

            # Contador para almacenar la cantidad de carpetas eliminadas
            folders_deleted = 0

            # Verificar si el directorio Temp existe antes de eliminarlo
            if os.path.exists(temp_path):
                # Obtener la lista de procesos en ejecución
                running_processes = [p.name() for p in psutil.process_iter()]

                # Recorrer los archivos y directorios en Temp para eliminar solo los que no están en uso
                for root, dirs, files in os.walk(temp_path, topdown=False):
                    for name in files + dirs:
                        path = os.path.join(root, name)
                        try:
                            # Verificar si el archivo/directorio está en uso por otro proceso
                            if os.path.exists(path) and os.path.normpath(path) not in running_processes:
                                if os.path.isfile(path):
                                    os.remove(path)
                                else:
                                    shutil.rmtree(path)
                                folders_deleted += 1
                        except Exception as e:
                            exceptions.append({'path': path, 'exception': e})

            print(f"Cleaned {folders_deleted} temporary file folders. {len(exceptions)} exception(s) found✅")
        else:
            print("Error: APPDATA environment variable not found.")
    except Exception as e:
        print(f"Error cleaning temporary files: {e}")
        return None

