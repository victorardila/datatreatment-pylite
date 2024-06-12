#!/bin/bash
# Script para abrir una ventana de selección de procesos de depuración de un dataframe
# Autor: Victor Ardila

# Obtiene la ruta del directorio donde se encuentra este script
current_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Declaración de opciones
options=(
  "quitar_caracteres_especiales"
  "eliminar_filas_duplicadas"
  "eliminar_columnas_duplicadas"
  "eliminar_filas_nulas"
  "eliminar_columnas_nulas"
  "llenar_celdas_vacias"
  "formatear_fecha"
  "convertir_caracteres_especiales"
  "convertir_a_valor_absoluto"
)

# Inicialmente, ninguna opción está seleccionada
selected_options=()

# Estado por defecto
default_status="pendiente"

# Definir colores
BRIGHT_GREEN='\033[1;32m'
BRIGHT_BLUE='\033[1;34m'
BRIGHT_ORANGE='\033[1;33m' 
BRIGHT='\033[1m'
NC='\033[0m' # No Color

# Función para formatear el texto
format_option() {
  local option="$1"
  option="${option//_/ }" # Reemplazar _ por espacios
  option="${option^}" # Convertir la primera letra a mayúscula
  echo "$option"
}

# Función para verificar si una opción está seleccionada
is_selected() {
  local option="$1"
  for selected in "${selected_options[@]}"; do
    if [ "$selected" == "$option" ]; then
      return 0
    fi
  done
  return 1
}

# Función para mostrar el menú de selección
show_menu() {
  echo -e "${BRIGHT}MENU DEBUG${NC}"
  echo "Seleccione un número para alternar la selección de una opción."
  echo "Presione <Intro> sin seleccionar nada para finalizar la selección."
  echo
  echo -e "${BRIGHT_GREEN}Seleccionar        Tipo de depuración                    Estado"
  echo -e "${BRIGHT_BLUE}-----------------------------------------------------------------"
  for i in "${!options[@]}"; do
    formatted_option=$(format_option "${options[i]}")
    if is_selected "${options[i]}"; then
      printf "${BRIGHT_GREEN}✔${NC} ${BRIGHT_ORANGE}%-10s${NC} ${BRIGHT}%-30s${NC} ${BRIGHT_ORANGE}%s${NC}\n" "$i" "$formatted_option" "            $default_status"
    else
      printf "  ${BRIGHT_BLUE}%-10s${NC} %-30s\n" "$i" "$formatted_option"
    fi
  done
  echo
}

# Bucle para mostrar el menú de selección y alternar las opciones seleccionadas
while true; do
  clear
  show_menu
  read -p "Pulse un número de selección: " choice

  if [ -z "$choice" ]; then
    # Si el usuario presiona Enter sin seleccionar, finalizar la selección
    break
  elif [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 0 ] && [ "$choice" -lt "${#options[@]}" ]; then
    # Si el usuario selecciona una opción válida, alternar la selección de la opción
    option="${options[$choice]}"
    if is_selected "$option"; then
      # Si la opción ya está seleccionada, deseleccionarla
      selected_options=("${selected_options[@]/$option}")
    else
      # Si la opción no está seleccionada, seleccionarla
      selected_options+=("$option")
    fi
  else
    # Mensaje de error para entrada no válida
    echo "Selección no válida, por favor intente de nuevo."
    sleep 1
  fi
done

# Guardar la lista de procesos seleccionados en un archivo .txt en la ruta del script
output_file="$current_dir/selectedProcesses.txt"
echo "Procesos seleccionados:" > "$output_file"
for process in "${selected_options[@]}"; do
  echo "$process" >> "$output_file"
done

echo "Lista de procesos seleccionados guardada en $output_file"