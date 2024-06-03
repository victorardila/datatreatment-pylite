import sys
import subprocess
import shutil
import os

def read_requirements():
    requirements = []
    if sys.platform.startswith('win'):
        requirements.append('pygetwindow')
    elif sys.platform.startswith('linux'):
        requirements.append('ewmh')
        requirements.append('python-xlib')
    return requirements

def main():
    requirements = read_requirements()
    for req in requirements:
        subprocess.check_call([sys.executable, "-m", "pip", "install", req])
    remove_pycache()

def remove_pycache():
    pycache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '__pycache__')
    if os.path.exists(pycache_dir):
        shutil.rmtree(pycache_dir)
        print(f"Eliminado {pycache_dir}")
    else:
        print("No se encontró __pycache__")

if __name__ == "__main__":
    main()
