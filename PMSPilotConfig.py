import os
import json

class PMSPilotConfig(dict):
    def __init__(self, path):
        self.__path__ = path
        super().__init__({})
        if os.path.exists(path):
            self.Read()
        
    def Read(self):
        with open(self.__path__) as f:
            super().__init__(json.load(f))            

    def Write(self):
        with open(self.__path__, 'w') as f:
            json.dump(self, f, indent=2)    
