@echo off
REM Script para abrir una ventana de selección de procesos de depuración de un dataframe
REM Autor: Victor Ardila

REM Declaración de opciones
setlocal enabledelayedexpansion
set options[0]="eliminar_filas_duplicadas"
set options[1]="eliminar_columnas_duplicadas"
set options[2]="eliminar_filas_nulas"
set options[3]="eliminar_columnas_nulas"
set options[4]="llenar_celdas_vacias"
set options[5]="quitar_caracteres_especiales"
set options[6]="formatear_fecha"
set options[7]="formatear_a_entero"

REM Inicialmente, ninguna opción está seleccionada
set "selected_options="

REM Estado por defecto
set "default_status=pendiente"

REM Función para mostrar el menú de selección
:show_menu
echo MENU DEBUG
echo Seleccione un número para alternar la selección de una opción.
echo Presione <Intro> sin seleccionar nada para finalizar la selección.
echo.
echo Seleccionar        Tipo de depuración                    Estado
echo -----------------------------------------------------------------
set /a max_length=0
for /L %%i in (0,1,7) do (
    set option=!options[%%i]!
    set formatted_option=!option:_= !
    set formatted_option=!formatted_option:~0,1!!formatted_option:~1!
    if "!formatted_option!"=="" set formatted_option=!option!
    if "!formatted_option!"=="" set formatted_option=opcion_vacia
    if "!formatted_option!"=="" set formatted_option=!option!
    if "!formatted_option!"=="eliminar_filas_duplicadas" set formatted_option=Eliminar filas duplicadas
    if "!formatted_option!"=="eliminar_columnas_duplicadas" set formatted_option=Eliminar columnas duplicadas
    if "!formatted_option!"=="eliminar_filas_nulas" set formatted_option=Eliminar filas nulas
    if "!formatted_option!"=="eliminar_columnas_nulas" set formatted_option=Eliminar columnas nulas
    if "!formatted_option!"=="llenar_celdas_vacias" set formatted_option=Llenar celdas vacías
    if "!formatted_option!"=="quitar_caracteres_especiales" set formatted_option=Quitar caracteres especiales
    if "!formatted_option!"=="formatear_fecha" set formatted_option=Formatear fecha
    if "!formatted_option!"=="formatear_a_entero" set formatted_option=Formatear a entero
    echo !i!            !formatted_option!            !default_status!
    set "option_length=!formatted_option!"
    setlocal enabledelayedexpansion
    set /a option_length=!option_length!
    endlocal
    if !option_length! gtr !max_length! (
        set "max_length=!option_length!"
    )
)
echo.
exit /b %max_length%
