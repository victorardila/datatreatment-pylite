import sys

class PlatformsSys:
    def __init__(self):
        # operatingSystem es una variable que almacena el sistema operativo en el que se está ejecutando el programa
        self.operatingSystem = ""
        self.detect_platform()

    def detect_platform(self):
        if sys.platform.startswith('win'):
            self.operatingSystem = "Windows"
        elif sys.platform.startswith('linux'):
            self.operatingSystem = "Linux"

    def get_operatingSystem(self):
        return self.operatingSystem