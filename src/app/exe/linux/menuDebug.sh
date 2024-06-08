#!/bin/bash
# Script para abrir una ventana de selección de procesos de depuración de un dataframe
# Autor: Victor Ardila

# Declaración de opciones
options=(
  "eliminar_filas_duplicadas"
  "eliminar_columnas_duplicadas"
  "eliminar_filas_nulas"
  "eliminar_columnas_nulas"
  "llenar_celdas_vacias"
  "quitar_caracteres_especiales"
  "formatear_fecha"
  "formatear_a_entero"
)

# Inicialmente, ninguna opción está seleccionada
selected_options=()

# Estado por defecto
default_status="pendiente"

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
  echo "MENU DEBUG"
  echo "Seleccione un número para alternar la selección de una opción."
  echo "Presione <Intro> sin seleccionar nada para finalizar la selección."
  echo
  echo "Seleccionar        Tipo de depuración                    Estado"
  echo "-----------------------------------------------------------------"
  for i in "${!options[@]}"; do
    formatted_option=$(format_option "${options[i]}")
    if is_selected "${options[i]}"; then
      echo "✓ $i            $formatted_option            $default_status"
    else
      echo "  $i            $formatted_option"
    fi
  done
  echo
}

# Bucle para mostrar el menú de selección y alternar las opciones seleccionadas
while true; do
  clear
  show_menu
  read -p "Pulse <Intro> para finalizar la selección o pulse un número de selección: " choice

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

# Mostrar las opciones seleccionadas con el estado "pendiente"
echo "Ha seleccionado las siguientes opciones:"
for option in "${selected_options[@]}"; do
  formatted_option=$(format_option "$option")
  echo "- $formatted_option (Estado: $default_status)"
done

# Devolver las opciones seleccionadas
echo "${selected_options[@]}"
