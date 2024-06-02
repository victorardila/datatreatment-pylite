import subprocess
import os

class RunFrontendReact:
    def __init__(self, path):
        self.path = path
        self.command = "npm start"

    def run(self):
        # Verifica si Node.js está instalado
        try:
            subprocess.run(["node", "-v"], check=True)
        except subprocess.CalledProcessError:
            print("Node.js no está instalado. Instálalo antes de continuar.")
            exit(1)

        # Verifica si npm está instalado
        try:
            subprocess.run(["npm", "-v"], check=True)
        except subprocess.CalledProcessError:
            print("npm no está instalado. Instálalo antes de continuar.")
            exit(1)

        # Cambia al directorio del proyecto de React
        os.chdir(self.path)

        # Instala las dependencias del proyecto de React
        try:
            subprocess.run(["npm", "install"], check=True)
        except subprocess.CalledProcessError:
            print("Error al instalar las dependencias.")
            exit(1)

        # Ejecuta el proyecto de React
        try:
            subprocess.run(["npm", "start"], check=True)
        except subprocess.CalledProcessError:
            print("Error al ejecutar el proyecto de React.")
            exit(1)
