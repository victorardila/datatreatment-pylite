class CollectionsGroupModel:
    # Constructor
    def __init__(self, name, collections):
        self.name = name
        self.collections = collections

    def __repr__(self):
        return f"{self.name}: {self.collections}"
    
    def __add__(self, other):
        if isinstance(other, CollectionsGroupModel):
            self.name.append(other.name)
            self.collections.append(other.collections)
        else:
            raise ValueError("El objeto debe ser una instancia de CollectionsGroupModel")
        return self
    
    def __getitem__(self, name):
        return self.collections[self.name.index(name)]
    
    def __setitem__(self, name, collection):
        self.collections[self.name.index(name)] = collection
    
    def __delitem__(self, name):
        index = self.name.index(name)
        self.name.pop(index)
        self.collections.pop(index)
    
    def __contains__(self, name):
        return name in self.name
    
    def __iter__(self):
        return iter(zip(self.name, self.collections))
