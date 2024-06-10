#!/bin/bash
# Script para abrir una ventana de selección de carpetas y obtener la ruta de la carpeta seleccionada
# Autor: Victor Ardila

# Comando para abrir una ventana de selección de carpetas usando Zenity
carpeta_seleccionada=$(zenity --file-selection --directory --title="Seleccionar carpeta")

# Verificar si se seleccionó una carpeta
if [ -z "$carpeta_seleccionada" ]; then
    echo "false"
else
    echo "$carpeta_seleccionada"
fi
