class CollectionsGroupModel:
    def __init__(self, name=None, collections=None):
        self.name = name if name is not None else []  # Lista de nombres
        self.collections = collections if collections is not None else []  # Lista de colecciones

    def __repr__(self):
        return f"CollectionsGroupModel({self.name}, {self.collections})"

    def add_collection(self, name, collection):
        self.name.append(name)
        self.collections.append(collection)

    def get_collection(self, name):
        try:
            index = self.name.index(name)
            return self.collections[index]
        except ValueError:
            return None  # El nombre no existe en la lista

    def set_collection(self, name, collection):
        try:
            index = self.name.index(name)
            self.collections[index] = collection
        except ValueError:
            raise ValueError(f"No se encontró la colección con el nombre {name}")

    def remove_collection(self, name):
        try:
            index = self.name.index(name)
            self.name.pop(index)
            self.collections.pop(index)
        except ValueError:
            raise ValueError(f"No se encontró la colección con el nombre {name}")

    def __contains__(self, name):
        return name in self.name

    def __iter__(self):
        return iter(zip(self.name, self.collections))
