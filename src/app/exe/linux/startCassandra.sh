#!/bin/bash
# Script para iniciar el servidor de Cassandra
# Autor: Victor Ardila

# Obtener la ruta del script actual
ruta_actual=$(dirname "$(readlink -f "$0")")

# Cambiar al directorio padre del directorio actual
cd "$ruta_actual/../../"

# Ruta al directorio de Cassandra
ruta_cassandra="$PWD/database/apache-cassandra-local/bin"

# Iniciar Cassandra en una nueva terminal
gnome-terminal --working-directory="$ruta_cassandra" -- bash -c "./cassandra"
