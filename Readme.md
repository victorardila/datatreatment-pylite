# Backend PyLite DataTreatment
Backend hecho en python para bases de datos documentales`(MongoDB)` y columnares`(cassandra)`

![MongoDB-and-Cassandra-removebg-preview](https://github.com/Valfonsoardila10/Backend-PyLite-Cassandra/assets/89551043/957a7f67-a2da-4ea8-b8de-d0e2c7050303)

# Requisitos para ejecutar el backend de Python

Este proyecto de backend está desarrollado en Python y requiere ciertas dependencias y la versión específica de Python para ejecutarse correctamente.

## Version de python: [`Python 3.11.5`](https://www.python.org/downloads/release/python-3115/)

## En widnows

```bash
curl -o python-3.11.10.exe https://www.python.org/ftp/python/3.11.10/python-3.11.10-amd64.exe
.\python-3.11.10.exe /quiet InstallAllUsers=1 PrependPath=1
python --version
```

## En linux

```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11
```

## Estrucutra del proyecto

El proyecto está estructurado de la siguiente manera:

```bash
backend/
├── public/
│  ├── connections/
│  ├── docs/
│  └── utils/
├── src/
│  ├── database/
│  │  ├── apache-cassandra-local/
│  ├── app/
│  │  ├── bat/
│  │  │  ├── linux/
│  │  │  │  ├── menuDebug.sh
│  │  │  │  ├── pathFile.sh
│  │  │  │  └── startCassandra.sh
│  │  │  ├── windows/
│  │  │  │  ├── menuDebug.bat
│  │  │  │  ├── pathFile.bat
│  │  │  │  └── startCassandra.bat
│  │  ├── dbOperations/
│  │  │  │  ├── drop.py
│  │  │  │  ├── insert.py
│  │  │  │  ├── keyspace.py
│  │  │  │  ├── select.py
│  │  │  │  ├── table.py
│  │  │  │  └── update.py
│  │  ├── scripts/
│  │  │  │  ├── csvManager.py
│  │  │  │  ├── debugCSV.py
│  │  │  │  └── postProcessing.py
│  ├── routes/
│  │  ├── close.connection.py
│  │  ├── create.keyspace.py
│  │  ├── create.table.py
│  │  ├── delete.table.py
│  │  ├── insert.data.py
│  │  ├── select.data.py
│  │  └── update.data.py
│  ├── config.py
│  ├── connection.py
│  ├── database.py
│  └── server.py
├── .gitignore
├── extras_packages.py
├── main.py
├── pyproject.toml
└── Readme.md
```
## Base de datos local
## [`Apache Cassandra 3.11.10`](https://archive.apache.org/dist/cassandra/3.11.10/)

La base de datos local se encuentra en la carpeta `apache-cassandra-local` y se debe ejecutar en un entorno local para que el backend pueda conectarse a ella.

### `Nota: Si la carpeta no existe, debe crearla y descargar la base de datos de Apache Cassandra 3.11.10 en ella.` 

## Base de datos en la nube
## [`MongoDB Atlas`](https://www.mongodb.com/es/cloud/atlas/register)

MongoDB Atlas es una base de datos en la nube que se puede utilizar para almacenar los datos de la aplicación. Se debe crear una cuenta en MongoDB Atlas y configurar la base de datos en la nube para que el backend pueda conectarse a ella.

### `Nota: debe configurar la base de datos en la nube y obtener las credenciales de conexión para que el backend pueda conectarse a ella.`

## Documentos y utilidades
En la carpeta public se encuentran las carpetas `connections`, `docs` y `utils` contienen los archivos de conexión, documentación y utilidades necesarios para el backend.

## Pasos de instalacion

## 1. Crear y Activar el entorno virtual

El entorno virtual se puede crear con venv
- Ejecute el siguiente comando para crear el entorno virtual e instalar dependencias:

### Instalacion en windows
```bash
python -m venv .venv
```
- Activar el entorno virtual:
```bash
.venv/bin/activate
```

- Desactivar el entorno virtual:
```bash
# muevete a la carpeta .venv
cd .venv/Scripts
# ejecuta el archivo deactivate
./deactivate
```
- Regenerar el archivo de bloqueo
```bash
poetry lock
```

### Instalacion de linux
```bash
python3 -m venv .venv
```
- Activar el entorno virtual:
```bash
source .venv/bin/activate
```

- Desactivar el entorno virtual:
```bash
deactivate
```

- Regenerar el archivo de bloqueo
```bash
poetry lock
```

## 2. Dependencias del proyecto

Las dependencias del proyecto pueden instalarse utilizando poetry. 

### `Nota: Antes debe asegurarse que su archivo pyproject.toml esté en la raíz del proyecto y de haber creado el entorno virtual.`

Ejecute el siguiente comando para crear el entorno virtual e instalar dependencias:

### Instalacion para windows
```bash
Invoke-WebRequest -Uri https://install.python-poetry.org -OutFile install-poetry.py
python install-poetry.py
```

- Instalar dependencias extras para windows:

```bash
poetry lock # Regenerar el archivo de bloqueo
poetry install --extras windows
```

### Instalacion para linux
```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry env use python3.11
```
- Instalar dependencias extras para linux:
```bash
poetry lock # Regenerar el archivo de bloqueo
poetry install
poetry run extras
```
## `Opcional para activar el entorno virtual con poetry es lo mismo que activar manualmente el .venv`
- Activar el entorno virtual 
```bash
poetry shell
```

- Desactivar el entorno virtual:
```bash
exit
```

## Comandos de ayuda

### Lista de utilidades: <img src="https://github.com/VictorArdila/VictorArdila/assets/89551043/25d307e3-ef06-41e0-8cb1-a979f4f130ac" alt="GitFlow" width="25" height="25"> [Git flow](https://github.com/VictorArdila/VictorArdila/blob/main/doc/GitFlow.md)





