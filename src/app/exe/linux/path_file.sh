#!/bin/bash
# Script para abrir una ventana de selección de archivos y obtener la ruta del archivo seleccionado
# Autor: Victor Ardila

# Comando para abrir una ventana de selección de archivos usando Zenity
archivo_seleccionado=$(zenity --file-selection --title="Seleccionar archivo")

# Verificar si se seleccionó un archivo
if [ -z "$archivo_seleccionado" ]; then
    echo "false"
else
    echo "$archivo_seleccionado"
fi
