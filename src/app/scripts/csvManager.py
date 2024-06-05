# -*- coding: utf-8 -*-
from src.app.scripts.checkerManager import checkColumnOutline, checkTableOutline, checkExistenceOfTables, checkExistenceOfColumns, checkExistenceOfkeyspace
from src.models.collectionesGroup import CollectionsGroupModel
from colorama import Style
from src.models.platformsSys import PlatformsSys
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from uuid import uuid4
import warnings
import subprocess
import requests
import os
import io

# Functions of cassandraManager
def copyCSVToCassandra(keyspace, table, csv_file, session):
  """
  Sube un archivo CSV a una tabla Cassandra utilizando el comando COPY.

  Args:
    keyspace: Nombre del keyspace.
    table: Nombre de la tabla.
    csv_file: Ruta al archivo CSV.
    session: Sesión de Cassandra.
  """

  try:
    # Crear la consulta COPY
    query = f"""
    COPY {keyspace}.{table}
    FROM '{csv_file}'
    WITH HEADER
    FORMAT CSV;
    """
    # Ejecutar la consulta
    session.execute(query)
    message = f"Datos del archivo CSV '{csv_file}' subidos a la tabla {keyspace}.{table} exitosamente✅"
  except Exception as e:
    message = f"Error al subir el archivo CSV a Cassandra: {e}🚫"

  return message

def uploadCSVToCassandra(keyspace, tables, typeData, debugData, session):
    """
    Sube los datos de un DataFrame a una base de datos Cassandra.

    Args:
        keyspace: Nombre del keyspace.
        tables: Diccionario con los nombres de las tablas.
        typeData: Diccionario con los tipos de datos.
        debugData: DataFrame con los datos limpios.
        primaryKeys: Diccionario con las claves primarias.
        sessionServer: Cassandra session object.
    """
    try:
        # Create keyspace (unchanged)
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 2}}")
        groupedColumns = {value.lower(): [key.lower() for key, val in tables.items() if val == value] for value in set(tables.values())}
        # Crear el diccionario modifiedTypeData con todas las entradas de typeData
        modifiedTypeData = {key.replace(" ", "_").replace(",_", " ").lower(): value for key, value in typeData.items()}
        # Create tables (unchanged)
        for key, values in groupedColumns.items():
            values = [value.replace("[","").replace("]","").replace(" ", "_").replace(",_", " ") for value in values]
            # Crear tabla vacia
            query = f"CREATE TABLE IF NOT EXISTS {keyspace}.{key} (id UUID PRIMARY KEY)"
            session.execute(query)
            columns, message = checkColumnOutline(session, key, keyspace)
            numColumns = len(columns) 
            print(f"Number of columns in {key}: {numColumns}")
            if numColumns <= 1:
                # Agregar columnas a la tabla
                query = f"ALTER TABLE {keyspace}.{key} ADD ("
                first_column = True
                for value in values:
                    if first_column:
                        # Skip comma for the first column
                        first_column = False
                    else:
                        query += ","  # Add comma for subsequent columns
                    # Check for data type in modifiedTypeData
                    if value.lower() in modifiedTypeData:
                        dataType = modifiedTypeData[value.lower()]
                    else:
                        dataType = "TEXT"
                    query += f"{value} {dataType}"
                query += ")"
                print(query)
                session.execute(query)       
            # Subir los datos a la tabla
            total_rows = len(debugData)
            batch_size = 100000
            num_batches = (total_rows + batch_size - 1) // batch_size
            with tqdm(total=num_batches, desc="Inserting data in batches") as pbar:
                for batch_start in range(0, total_rows, batch_size):
                    batch_end = min(batch_start + batch_size, total_rows)
                    batch_data = debugData.iloc[batch_start:batch_end]
                    for index, row in batch_data.iterrows():
                        for key, values in groupedColumns.items():
                            uid =  uuid4()
                            # Procesar los valores de las columnas dandole formato
                            processed_values = ["id"] + [value.replace("[", "").replace("]", "").replace(" ", "_").replace(",_", " ") for value in values]
                            typeData = {key.replace(" ", "_").replace(",_", " ").lower(): value for key, value in typeData.items()}
                            row_values = [uid] + [row.get(value, None) for value in processed_values[1:]]
                            # Determinar como se insertaran los valores en la tabla segun el tipo de dato
                            type_formatters = {
                                "TEXT": lambda x: f"'{x}'",
                                "TIMESTAMP": lambda x: f"'{x}'",
                                "FLOAT": lambda x: f"{x}",
                                "INT": lambda x: f"{x}",
                                "BOOLEAN": lambda x: f"{x}",
                                "DOUBLE": lambda x: f"{x}",
                            }
                            for i, value in enumerate(row_values):
                                if i < len(processed_values):
                                    data_type = typeData.get(processed_values[i], "TEXT")  # Por defecto, asume tipo TEXT si no se encuentra en typeData
                                    row_values[i] = type_formatters.get(data_type, type_formatters["TEXT"])(value)
                            # Crear la consulta INSERT INTO
                            query = f"INSERT INTO {keyspace}.{key} ("+", ".join(processed_values) + ") VALUES (" + ", ".join(row_values) + ")"
                            # print(query)
                            session.execute(query)  
                    pbar.update(1)  # Actualizar la barra de progreso después de cada lote
        message = "Datos subidos a Cassandra exitosamente✅"
        return message
    except Exception as e:
        # Atrapar la fila que causó el error
        message = f"Error al subir los datos a Cassandra: {e}🚫"
        return message

# Functions of mongoClusterManager
def transformUploadData(dataframe, structures, client):
    """
    Transforma un DataFrame en listas de diccionarios basados en las estructuras de JSON proporcionadas.
    
    Args:
        dataframe: DataFrame de pandas con todas las columnas necesarias.
        collections: Lista de objetos CollectionsGroupModel con las estructuras de JSON.
    
    Retorno:
        List of CollectionsGroupModel: Lista de objetos CollectionsGroupModel con estaciones y muestras.
    """
    # Solo subira 562500 registros por año esto es para la coleccion muestras
    # El año inicial sera el primer registro de la columna fecha esto es para la coleccion muestras
    # Tomara cada estructura de structures para formatear los registros del dataframe
    # Las estaciones no se repetiran seran unicas me refiero a que no deben haber dos estaciones con el mismo nombre
    # Cada estacion tendra en el campo departamentos un array con los departamentos que tiene
    # Cada estacion tendra en el campo municipios un array con los municipios que tiene
    # Cada muestra tendra un campo llamado estacion que seran los datos de la estacion como el nombre, el codigo, la latitud y longitud
    # Al final collections tendra el nombre de la coleccion que sera el nombre de la estructura y los jsons de esa coleccion
    try:   
        collections = CollectionsGroupModel()
        estaciones_dict = {}
        
        for structure in structures:
            json_structure = structure["schema"]
            collection_name = structure["name"]
            if collection_name == "estacion":
                departamentos = set()
                municipios = set()
                jsons_station_list = []
                # For tqdm progress bar
                for index, row in tqdm(dataframe.iterrows(), total=len(dataframe), desc=f"Procesando estaciones {collection_name}"):
                    if row['codigo_del_departamento'] not in departamentos:
                        # agreara jsons a departamentos
                        departamentos.add((row['codigo_del_departamento'], row['departamento']))
                    if row['codigo_del_municipio'] not in municipios:
                        municipios.add((row['codigo_del_municipio'], row['nombre_del_municipio']))                
                    json_stationn = {}
                    for key, value in json_structure.items():
                        if key == "departamentos":
                            # convertir la lista de departamentos a un json
                            json_departamentos = []
                            for i, (codigo, departamento) in enumerate(departamentos):
                                json_departamento = {
                                    "codigo_del_departamento": codigo,
                                    "departamento": departamento
                                }
                                print(json_departamento)
                                json_departamentos.append(json_departamento)
                                print(json_departamentos)
                            json_stationn[key] = json_departamentos
                    #     elif key == "municipios":
                    #         # convertir la lista de municipios a un json
                    #         json_municipios = []
                    #         for i, (codigo, municipio) in enumerate(municipios):
                    #             json_municipio = {
                    #                 "codigo_del_municipio": codigo,
                    #                 "nombre_del_municipio": municipio
                    #             }
                    #             json_municipios.append(json_municipio)
                    #         json_stationn[key] = json_municipios
                    #     else:
                    #         json_stationn[key] = row[value]
                    # print("Json: ", json_stationn)
                    # jsons_station_list.append(json_stationn)
                    # print("Lista de jsons: ", jsons_station_list)
                #collections.add_collection(name=collection_name, jsons=jsons_station_list)
                # Subir estaciones y obtener sus ObjectId
                # estaciones_dict = uploadDataToMongoCluster(collections.get_collections(), client, return_object_ids=True)
                #print(estaciones_dict)
            # elif collection_name == "muestra":
            #     stopIndexPerYear = 562500
            #     year_counters = {}
            #     # For tqdm progress bar
            #     for index, row in tqdm(dataframe.iterrows(), total=len(dataframe), desc=f"Procesando muestras {collection_name}"):
            #         current_year = str(row['fecha'])[:4]
            #         if current_year not in year_counters: 
            #             year_counters[current_year] = 0
            #         if year_counters[current_year] < stopIndexPerYear:
            #             json_muestras = {}
            #             for key, value in json_structure.items():
            #                 if key == "estacion":
            #                     estacion_id = ""#estaciones_dict.get(row['nombre_de_la_estacion'])
            #                     json_muestras[key] = {
            #                         "objectId": estacion_id,
            #                         "nombre_de_la_estacion": row['nombre_de_la_estacion'],
            #                         "latitud": row['latitud'],
            #                         "longitud": row['longitud']
            #                     }
            #                 else:
            #                     json_muestras[key] = row[key]
            #             print(json_muestras)
            #             collections.add_collection(name=collection_name, jsons=json_muestras)
            #             year_counters[current_year] += 1
            #     uploadDataToMongoCluster(collections.get_collections(), client)
    except Exception as e:
        message=f"Error al transfromar datos: {e}"
        print(message)
    
def uploadDataToMongoCluster(collections_list, client, return_object_ids=False):
    """
    Sube los datos de un DataFrame a una base de datos MongoDB.

    Args:
        collections_list: Lista de colecciones y sus datos.
        client: Cliente de MongoDB.
        return_object_ids: Si es True, devuelve un diccionario con los ObjectId de las estaciones.

    Retorno:
        Diccionario de ObjectId de las estaciones si return_object_ids es True.
    """
    try:
        db = client["air_quality"]
        object_ids = {}
        for name, collection_data in collections_list:
            collection = db[name]
            if return_object_ids and name == "estaciones":
                result = collection.insert_many(collection_data)
                for doc, object_id in zip(collection_data, result.inserted_ids):
                    object_ids[doc['nombre_de_la_estacion']] = object_id
            else:
                collection.insert_many(collection_data)
        message = f"Datos subidos a la colección {collection} exitosamente✅"
        if return_object_ids:
            return object_ids
    except Exception as e:
        message = f"Error al subir los datos a MongoDB: {e}🚫"
    return message

# Functions of csvManager
def get_file_size(path):
    """
    Obtiene el tamaño de un archivo.

    Args:
        path: Ruta del archivo.

    Returns:
        El tamaño del archivo en bytes.
    """
    try:
        # Obtener el tamaño del archivo
        with open(path, 'r') as f:
            size = f.seek(0, io.SEEK_END)
            f.seek(0)
        return size
    except Exception as e:
        print(f"Error al obtener el tamaño del archivo: {e}🚫")
        return None

def getPathCSV():
    """
    Obtiene la ruta del archivo CSV a partir de un archivo ejecutable.

    Returns:
        La ruta del archivo CSV.
    """
    try:
        # Obtener la ruta del archivo actual
        ruta_actual = Path(__file__).parent
        # Subir dos niveles
        ruta_dos_niveles_arriba = ruta_actual.parent.parent
        # Obtengo el tipo de sistema operativo
        platformsSys = PlatformsSys()
        operatingSystem = platformsSys.get_operatingSystem()
        ruta_exe = (ruta_dos_niveles_arriba / 'app' / 'exe' / 'windows' / 'path_file.bat') if operatingSystem == "Windows" else (ruta_dos_niveles_arriba / 'app' / 'exe' / 'linux' / 'path_file.sh')
        # Ejecutar el archivo .bat y capturar la salida
        proceso = subprocess.Popen([ruta_exe], stdout=subprocess.PIPE)
        salida_bytes, _ = proceso.communicate()
        # Decodificar la salida del proceso .bat
        salida = salida_bytes.decode('utf-8').strip()
        # Esperar a que el proceso termine o se cierre manualmente
        proceso.wait()
        if salida.__contains__('false'):
            message = 'Se cerró el proceso manualmente.'
            return message, None
        else:
            # Mostrar todos los archivos en la ruta obtenida
            archivos_en_ruta = os.listdir(salida)
            archivos_csv = [archivo for archivo in archivos_en_ruta if archivo.endswith('.csv')]
            if archivos_csv:
                # Si el archivo csv con el _clean al final existe tomar esa ruta sino tomar la primera ruta
                path = [archivo for archivo in archivos_csv if archivo.endswith('_clean.csv')]
                ruta_csv = None
                if path:
                    ruta_csv = os.path.join(salida, path[0])
                else:
                    ruta_csv = os.path.join(salida, archivos_csv[0])
                message = f"Archivo CSV encontrado en 📁 : {ruta_csv}"
                return message, ruta_csv
            else:
                message = 'No se encontraron archivos CSV en la ruta🚫'
                return message, None
    except Exception as e:
        message = f"Error al obtener la ruta del archivo CSV: {e}🚫"
        return message, None

def getCSVData(path):
    """
    Lee un archivo CSV y devuelve los encabezados y los datos en un DataFrame.

    Args:
        path: Ruta del archivo CSV.

    Returns:
        Una tupla con los encabezados y los datos del CSV.
    """
    try:
        headers = []
        warningsList = []
        total_rows = 0

        # Leer los encabezados del CSV sin tildes y transformarlos
        with open(path, 'r', encoding='utf-8') as f:
            headers = f.readline().strip().split(',')
            headers = [header.replace(" ", "_").replace(",_", " ").lower() for header in headers]
            headers = [header.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u") for header in headers]

        # Leer los datos del CSV por chunks
        chunks = []
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df_iter = pd.read_csv(path, low_memory=True, chunksize=100000, names=headers, encoding='utf-8', skiprows=1)
            for chunk in df_iter:
                chunks.append(chunk)
                total_rows += len(chunk)
                print(Style.NORMAL + f"{total_rows} rows read...📥")
                # Liberar memoria después de procesar cada chunk
                del chunk
            # Almacenar los warnings en la lista
            warningsList = [str(warning.message) for warning in w]
        # Agregar los chunks al DataFrame final
        data = pd.concat(chunks, ignore_index=True)
        message = f"\nCSV file read successfully. ⚠️  Warnings total: {len(warningsList)}. 🗄️  Total rows: {total_rows}"
        return message, data, warningsList
    except Exception as e:
        message = f"Error reading CSV file: {e}🚫"
        return message, None

def getWebCSVData(uri):
    """
    Descarga un archivo CSV de una URL y devuelve los encabezados y los datos en un DataFrame.

    Args:
        url: URL del archivo CSV.

    Returns:
        Una tupla con los encabezados y los datos del CSV.
    """
    try:
        # Descarga el archivo CSV
        response = requests.get(uri, stream=True)
        # Obtiene el tamaño del archivo
        total_size = int(response.headers['Content-Length'])
        # Crea una barra de progreso
        with tqdm.tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
            # Descarga el archivo en chunks
            for chunk in response.iter_content(chunk_size=1024):
                pbar.update(len(chunk))
                # Escribe el chunk al archivo
                with open('data.csv', 'ab') as csvfile:
                    csvfile.write(chunk)
        # Leer los datos del CSV
        data = pd.read_csv(io.StringIO(response.text))
        # Obtener los encabezados del CSV
        headers = data.columns
        return headers, data
    except Exception as e:
        print(f"Error downloading CSV file: {e}🚫")
        return None
    
def createCleanCSV(dataframe, path):
    """
    Crea un nuevo archivo CSV con los datos limpios.

    Args:
        dataframe: DataFrame con los datos limpios.
        path: Ruta donde se guardará el archivo CSV.
    """
    try:
        # tomar la ultima parte de la ruta que define el nombre del archivo
        path = path.split('/')[-1]
        # Si path no contiene la palabra clean, se le agrega
        if not path.__contains__('_clean'):
            path = path.replace('.csv', '_clean.csv')
        # Comprabar si el archivo no existe
        if not os.path.exists(path):
            # Calcular el total de filas del DataFrame
            total_rows = len(dataframe)
            # Crear la barra de progreso
            with tqdm(total=total_rows, desc="Guardando CSV", unit="fila") as pbar:
                # Definir el chunksize para dividir el DataFrame en partes más pequeñas
                chunksize = 1000000  # Por ejemplo, 1 millón de filas por chunk
                # Guardar el DataFrame como CSV en chunks para actualizar la barra de progreso
                for chunk in range(0, total_rows, chunksize):
                    dataframe.iloc[chunk:chunk + chunksize].to_csv(path, mode='a', index=False, header=not chunk, chunksize=chunksize)
                    # Actualizar la barra de progreso por cada chunk guardado
                    pbar.update(chunksize if chunk + chunksize <= total_rows else total_rows - chunk)
            message = f"Archivo CSV guardado en 📁 : {path}✅"
        else:
            message = f"El archivo CSV ya existe en 📁 : {path}✅"
        return message
    except Exception as e:
        message = f"Error al crear el archivo CSV: {e}🚫"
        return message

