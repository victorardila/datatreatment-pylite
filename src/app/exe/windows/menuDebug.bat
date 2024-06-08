@echo off
REM Script para abrir una ventana de selección de procesos de depuración de un dataframe
REM Autor: Victor Ardila

REM Declaración de opciones
setlocal EnableDelayedExpansion
set "options=eliminar_filas_duplicadas eliminar_columnas_duplicadas eliminar_filas_nulas eliminar_columnas_nulas llenar_celdas_vacias quitar_caracteres_especiales formatear_fecha formatear_a_entero"

REM Inicialmente, ninguna opción está seleccionada
set "selected_options="

REM Estado por defecto
set "default_status=pendiente"

REM Función para formatear el texto
:format_option
set "option=%1"
set "option=%option:_= %"
echo !option:~0,1!!option:~1!
goto :eof

REM Función para verificar si una opción está seleccionada
:is_selected
set "option=%1"
for %%i in (!selected_options!) do (
    if "%%i"=="%option%" (
        exit /b 0
    )
)
exit /b 1

REM Función para mostrar el menú de selección
:show_menu
cls
echo MENU DEBUG
echo Seleccione un número para alternar la selección de una opción.
echo Presione Enter sin seleccionar nada para finalizar la selección.
echo(
echo Seleccionar        Tipo de depuración                    Estado
echo -----------------------------------------------------------------

set "i=0"
for %%o in (%options%) do (
    call :format_option %%o
    set "formatted_option=!option!"
    call :is_selected %%o
    if !errorlevel! equ 0 (
        echo ✓  !i!            !formatted_option!            !default_status!
    ) else (
        echo    !i!            !formatted_option!
    )
    set /a i+=1
)
echo(
goto :eof

REM Bucle para mostrar el menú de selección y alternar las opciones seleccionadas
:menu_loop
call :show_menu
set /p "choice=Pulse Enter para finalizar la selección o un número de selección: "

if "%choice%"=="" (
    goto end_selection
) else (
    set /a choice_index=%choice%
    set "i=0"
    for %%o in (%options%) do (
        if "!i!"=="%choice_index%" (
            call :is_selected %%o
            if !errorlevel! equ 0 (
                REM Si la opción ya está seleccionada, deseleccionarla
                set "selected_options=!selected_options: %%o=!"
            ) else (
                REM Si la opción no está seleccionada, seleccionarla
                set "selected_options=!selected_options! %%o"
            )
        )
        set /a i+=1
    )
    goto menu_loop
)

:end_selection
REM Mostrar las opciones seleccionadas con el estado "pendiente"
echo Ha seleccionado las siguientes opciones:
for %%o in (%selected_options%) do (
    call :format_option %%o
    echo - !option! (Estado: !default_status!)
)

REM Retornar las opciones seleccionadas
set "result=%selected_options%"
endlocal & set "selected_options=%result%"
exit /b 0
