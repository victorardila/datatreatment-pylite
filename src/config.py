# Description: Configuration file for the backend
host = "localhost"
port = 9042
username = "cassandra"
password = "cassandra"

# Informacion de la base de datos
# Url del csv en la web
url = "https://www.datos.gov.co/api/views/ysq6-ri4e/rows.csv?accessType=DOWNLOAD"

# Nombre de la base de datos
keyspace = "data_air_quality"

# Familias de columnas
familys = [
    "fechas",
    "mediciones",
    "ubicaciones",
    "descripciones",
]

# Relaciones familiares entre las columnas
familyColumns = {
    #  Columnas | Familias
        "Fecha": "fechas",
        "Autoridad Ambiental": "descripciones",
        "Nombre de la estacion": "descripciones",
        "Tecnologia": "descripciones",
        "Latitud": "ubicaciones",
        "Longitud": "ubicaciones",
        "Codigo del departamento": "ubicaciones",
        "Departamento": "ubicaciones",
        "Codigo del municipio": "ubicaciones",
        "Nombre del municipio": "ubicaciones",
        "Tipo de estacion": "mediciones",
        "Tiempo de exposicion": "mediciones",
        "Variable": "mediciones",
        "Unidades": "mediciones",
        "Concentracion": "mediciones",
        "Nueva columna georreferenciada": "ubicaciones",
    }

# Columnas y tipos de datos
typeData = {
    #  Columnas | tipo de datos
        "Fecha": "TIMESTAMP",
        "Autoridad Ambiental": "TEXT",
        "Nombre de la estacion": "TEXT",
        "Tecnologia": "TEXT",
        "Latitud": "FLOAT",
        "Longitud": "FLOAT",
        "Codigo del departamento": "INT",
        "Departamento": "TEXT",
        "Codigo del municipio": "INT",
        "Nombre del municipio": "TEXT",
        "Tipo de estacion": "TEXT",
        "Tiempo de exposicion": "INT",
        "Variable": "TEXT",
        "Unidades": "TEXT",
        "Concentracion": "FLOAT",
        "Nueva columna georreferenciada": "TEXT",
    }

def getURL():
    return url

def getKeyspace():
    return keyspace

def getFamilys():
    return familys

def getTypeData():
    columns = []
    types = []
    for key,value in typeData.items():
        key=[key.replace(" ", "_").replace(",_", " ").lower() for key in typeData.keys()]
        columns.append(key)
        types.append(value)
    return columns, types

def getFamilyColumns():
    columns = []
    families = []
    for key,value in familyColumns.items():
        key=[key.replace(" ", "_").replace(",_", " ").lower() for key in familyColumns.keys()]
        columns.append(key)
        families.append(value)
    return columns, families
