# -*- coding: utf-8 -*-
from app.scripts.checkerManager import checkColumnOutline, checkTableOutline, checkExistenceOfTables, checkExistenceOfColumns, checkExistenceOfkeyspace
from src.models.groupCollections.model import GroupCollectionsModel
from colorama import Style
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
    Obtiene la ruta del archivo CSV a partir de un archivo BAT.

    Returns:
        La ruta del archivo CSV.
    """
    try:
        # Obtener la ruta del archivo actual
        ruta_actual = Path(__file__).parent
        # Subir dos niveles
        ruta_dos_niveles_arriba = ruta_actual.parent.parent
        # Ruta al archivo .bat
        ruta_bat = ruta_dos_niveles_arriba / 'app' / 'bat' / 'path_file.bat'
        # Ejecutar el archivo .bat y capturar la salida
        proceso = subprocess.Popen([ruta_bat], stdout=subprocess.PIPE)
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
            # Verificar si hay archivos .csv en la ruta
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
        # Leer el CSV por chunks
        chunks = []
        headers = []
        warningsList = []
        total_rows = 0
        # Leer los encabezados del CSV sin tildes
        with open(path, 'r', encoding='utf-8') as f:
            headers = f.readline().strip().split(',')
            # Reeemplazo espacio por guion bajo, coma seguido de guion bajo por espacio, y convierto a minúsculas
            headers = [header.replace(" ", "_").replace(",_", " ").lower() for header in headers]
            # Cambiar las tildes por letras sin tilde
            headers = [header.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u") for header in headers]
        # Capturar los warnings durante la lectura
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Leer los datos por chunks y concatenarlos
            df_iter = pd.read_csv(path, low_memory=True, chunksize=100000, names=headers, encoding='utf-8', skiprows=1)
            for chunk in df_iter:
                chunks.append(chunk)
                total_rows += len(chunk)
                print(Style.NORMAL + f"{total_rows} rows read...📥")
            # Almacenar los warnings en la lista
            for warning in w:
                warningsList.append(str(warning.message))
        # Agregar el header como encabezados al principio del DataFrame
        data = pd.concat(chunks, ignore_index=True)
        message = f"\nCSV file read successfully. ⚠️  Warnings total: {len(warningsList)}. 🗄️  Total rows: {total_rows}"
        del chunks # Liberar memoria
        return message, data, warningsList
    except Exception as e:
        message = f"Error reading CSV file: {e}🚫"
        return message, None

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
def transformDataframeToJson(df, collections):
    """
    Transforma un DataFrame en listas de diccionarios basados en las estructuras de JSON proporcionadas.
    
    Args:
        df: DataFrame de pandas con todas las columnas necesarias.
        json_estacion: Diccionario que define la estructura de los datos de "estacion".
        json_muestra: Diccionario que define la estructura de los datos de "muestra".
    
    Retorno:
        estaciones: Lista de diccionarios con la estructura de "estacion".
        muestras: Lista de diccionarios con la estructura de "muestra".
    """
    # collections: list object = C[name:"", schema: {}]
    estaciones = []
    muestras = []
    cantidadDeColecciones = range(collections)
    # collections son lista de objetos que tienen dos atributos: name y model
    for i in cantidadDeColecciones:
        json_estacion = collections[i].model[0]
        json_muestra = collections[i].model[1]
        # Uso de tqdm para mostrar el progreso
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Transformando datos"):
            estacion = {key: row[key] for key in json_estacion.keys()}
            muestra = {
                key: (row[key] if isinstance(json_muestra[key], str) 
                    else {sub_key: row[sub_key] for sub_key in json_muestra[key].keys()})
                for key in json_muestra.keys()
            }
            estaciones.append(estacion)
            muestras.append(muestra)
    # Nuevos objetos

    return estaciones, muestras

def uploadDataToMongoCluster(db, collection, dataFrame):
    """
    Sube los datos de un DataFrame a una base de datos MongoDB.

    Args:
        db: Nombre de la base de datos.
        collection: Nombre de la colección.
        dataFrame: DataFrame con los datos limpios.
    """
    try:
        # Convertir el DataFrame a una lista de diccionarios
        data = dataFrame.to_dict(orient='records')
        # Insertar los datos en la colección
        db[collection].insert_many(data)
        message = f"Datos subidos a la colección {collection} exitosamente✅"
    except Exception as e:
        message = f"Error al subir los datos a MongoDB: {e}🚫"
    return message

# Functions of csvManager
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
        # Comprabar si el archivo no existe
        if not os.path.exists(path):
            # Cambiamos el nombre del archivo a 'clean_data.csv'
            path = path.replace('.csv', '_clean.csv')
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

