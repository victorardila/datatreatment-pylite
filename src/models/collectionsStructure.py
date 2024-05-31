import json
import os


class CollectionsStructureModel():
    # Constructor
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        
    def __repr__(self):
        return {self.name: self.schema}
    
    def __add__(self, name, schema):
        self.name.append(name)
        self.schema.append(schema)
        
    def __load__(self, path):
        # Lista
        lista = []
        # obtener los archivos de la ruta
        files = os.listdir(path)
        # recorrer los archivos de la ruta para saber cuales son json
        for file in files:
            if file.endswith('.json'):
                # obtener los nombres de los archivos
                self.name = file.split('.')[0]
                # abrir el archivo con json.load
                with open(path + file, 'r') as f:
                    self.schema = json.load(f)
                # agregar a la lista
                lista.append({'name':self.name,'schema': self.schema})
        return lista
