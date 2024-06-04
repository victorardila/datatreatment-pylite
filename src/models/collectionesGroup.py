class CollectionsGroupModel:
    def __init__(self, name=None, jsons=None):
        self.name = name if name is not None else []  # Lista de nombres
        self.jsons = jsons if jsons is not None else []  # Lista de colecciones

    def __repr__(self):
        return f"CollectionsGroupModel(name={self.name}, jsons={self.jsons})"

    def add_collection(self, name, jsons):
        self.name.append(name)
        self.jsons.append(jsons)

    def get_collections(self):
        return self.jsons
    
    def get_collection(self, name):
        try:
            index = self.name.index(name)
            return self.jsons[index]
        except ValueError:
            return None  # El nombre no existe en la lista

    def set_collection(self, name, json):
        try:
            index = self.name.index(name)
            self.jsons[index] = json
        except ValueError:
            raise ValueError(f"No se encontró la colección con el nombre {name}")

    def remove_collection(self, name):
        try:
            index = self.name.index(name)
            self.name.pop(index)
            self.jsons.pop(index)
        except ValueError:
            raise ValueError(f"No se encontró la colección con el nombre {name}")

    def __contains__(self, name):
        return name in self.name

    def __iter__(self):
        return iter(zip(self.name, self.jsons))
