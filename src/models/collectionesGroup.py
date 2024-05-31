class CollectionsGroupModel():
    # Constructor
    def __init__(self, name, collections):
        self.name = name
        self.collections = collections
        
    def __repr__(self):
        return {self.name: self.collections}
    
    def __add__(self, name, collections):
        self.name.append(name)
        self.collections.append(collections)
        
    def __getitem__(self, name, collections):
        return self.collections[self.name.index(name)]
    
    def __setitem__(self, name, collections):
        self.collections[self.name.index(name)] = collections
    
    def __delitem__(self, name):
        index = self.name.index(name)
        self.name.pop(index)
        self.collections.pop(index)
    
    def __contains__(self, name):
        return name in self.name
    