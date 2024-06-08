@echo off
setlocal enabledelayedexpansion

:: Declarar opciones
set options=eliminar_filas_duplicadas eliminar_columnas_duplicadas eliminar_filas_nulas eliminar_columnas_nulas llenar_celdas_vacias quitar_caracteres_especiales formatear_fecha formatear_a_entero

:: Inicialmente, ninguna opción está seleccionada
set selected_options=

:: Estado por defecto
set default_status=pendiente

:: Función para formatear el texto (reemplazar _ por espacios y primera letra mayúscula)
set format_option=cmd /c "for %%a in (^^!opt^^!) do (set formatted_option=%%~na & set formatted_option=%%formatted_option:_= & call set formatted_option=%%formatted_option:~0,1%% %%formatted_option:~1%%)"

:: Función para verificar si una opción está seleccionada
:check_selected
set selected=0
for %%o in (%selected_options%) do (
    if "%%o"=="%~1" set selected=1
)
exit /b %selected%

:: Función para mostrar el menú de selección
:show_menu
cls
echo MENU DEBUG
echo Seleccione un número para alternar la selección de una opción.
echo Presione Enter sin seleccionar nada para finalizar la selección.
echo.
echo Seleccionar        Tipo de depuración                    Estado
echo -----------------------------------------------------------------
set i=0
for %%o in (%options%) do (
    set opt=%%o
    %format_option%
    call :check_selected %%o
    if !selected! equ 1 (
        echo [X] !i!            !formatted_option!            %default_status%
    ) else (
        echo [ ] !i!            !formatted_option!
    )
    set /a i+=1
)
echo.
exit /b

:: Bucle para mostrar el menú de selección y alternar las opciones seleccionadas
:loop
call :show_menu
set /p choice=Pulse Enter para finalizar la selección o pulse un número de selección: 
if "%choice%"=="" goto end
for /f "tokens=*" %%c in ("%choice%") do set /a choice=%%c
if %choice% geq 0 if %choice% lss 8 (
    set i=0
    for %%o in (%options%) do (
        if !i! equ %choice% (
            call :check_selected %%o
            if !selected! equ 1 (
                set selected_options=!selected_options:%%o =!
            ) else (
                set selected_options=!selected_options! %%o
            )
        )
        set /a i+=1
    )
) else (
    echo Selección no válida, por favor intente de nuevo.
    timeout /t 1 >nul
)
goto loop

:end
cls
echo Ha seleccionado las siguientes opciones:
if "%selected_options%"=="" (
    echo null
) else (
    for %%o in (%selected_options%) do (
        set opt=%%o
        %format_option%
        echo - !formatted_option! (Estado: %default_status%)
    )
)
echo.
if "%selected_options%"=="" (
    echo null
) else (
    echo %selected_options%
)
