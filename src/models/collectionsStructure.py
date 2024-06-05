import json
import os
from pathlib import Path

class CollectionsStructureModel:
    def __init__(self):
        self.structures = []  # Inicializar la lista de estructuras

    def load(self):
        # Obtener la ruta del archivo actual
        ruta_actual = Path(__file__).parent
        # Subir dos niveles
        ruta_dos_niveles_arriba = ruta_actual.parent.parent
        path = ruta_dos_niveles_arriba / 'public' / 'docs' / 'json'
        files = os.listdir(path)
        for file in files:
            if file.endswith('.json'):
                print(file)
                name = file.split('.')[0]
                with open(os.path.join(path, file), 'r') as f:
                    schema = json.load(f)
                self.structures.append({'name': name, 'schema': schema})
                print(self.structures)

    def __repr__(self):
        return str(self.structures)

    def add_structure(self, name, schema):
        self.structures.append({'name': name, 'schema': schema})

    def get_structures(self):
        return self.structures
