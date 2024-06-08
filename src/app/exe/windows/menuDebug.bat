@echo off
REM Script para abrir una ventana de selección de procesos de depuración de un dataframe
REM Autor: Victor Ardila

REM Declaración de opciones
set "options=eliminar_filas_duplicadas eliminar_columnas_duplicadas eliminar_filas_nulas eliminar_columnas_nulas llenar_celdas_vacias quitar_caracteres_especiales formatear_fecha formatear_a_entero"

REM Inicialmente, ninguna opción está seleccionada
set "selected_options="

REM Estado por defecto
set "default_status=pendiente"

REM Función para formatear el texto
:format_option
set "option=%~1"
set "option=%option:_= %" REM Reemplazar _ por espacios
set "option=%option:~0,1%%option:~1,-1%" REM Convertir la primera letra a mayúscula
echo %option%

REM Función para verificar si una opción está seleccionada
:is_selected
set "option=%~1"
for %%i in (%selected_options%) do (
  if /i "%%i"=="%option%" (
    exit /b 0
  )
)

REM Función para mostrar el menú de selección
:show_menu
echo MENU DEBUG
echo Seleccione un número para alternar la selección de una opción.
echo Presione <Intro> sin seleccionar nada para finalizar la selección.
echo.
echo Seleccionar        Tipo de depuración                    Estado
echo -----------------------------------------------------------------
for %%i in (%options%) do (
  call :format_option "%%i"
  if exist exit.txt exit
)
echo.
pause
exit /b
