from pymongo import MongoClient

# Conectar a la base de datos MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['your_database_name']

# Seleccionar las colecciones donde quieres insertar los datos
collection_1 = db['collection_name_1']
collection_2 = db['collection_name_2']

# Insertar los datos en las colecciones
collection_1.insert_many(json_structure_1)
collection_2.insert_many(json_structure_2)
