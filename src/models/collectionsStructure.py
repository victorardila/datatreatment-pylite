import json
import os

class CollectionsStructureModel:
    def __init__(self):
        self.structures = []  # Inicializar la lista de estructuras

    def load(self, path):
        files = os.listdir(path)
        for file in files:
            if file.endswith('.json'):
                name = file.split('.')[0]
                with open(os.path.join(path, file), 'r') as f:
                    schema = json.load(f)
                self.structures.append({'name': name, 'schema': schema})

    def __repr__(self):
        return str(self.structures)

    def add_structure(self, name, schema):
        self.structures.append({'name': name, 'schema': schema})

    def get_structures(self):
        return self.structures
