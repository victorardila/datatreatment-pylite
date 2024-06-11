@echo off
REM Script para abrir una ventana de selección de procesos de depuración de un dataframe
REM Autor: Victor Ardila

setlocal enabledelayedexpansion

:: Definir colores
set "BRIGHT_GREEN=^[[92m"
set "BRIGHT_BLUE=^[[94m"
set "BRIGHT_ORANGE=^[[93m"
set "BRIGHT=^[[1m"
set "NC=^[[0m"

:: Definir las opciones
set "options[0]=eliminar_filas_duplicadas"
set "options[1]=eliminar_columnas_duplicadas"
set "options[2]=eliminar_filas_nulas"
set "options[3]=eliminar_columnas_nulas"
set "options[4]=llenar_celdas_vacias"
set "options[5]=quitar_caracteres_especiales"
set "options[6]=formatear_fecha"
set "options[7]=formatear_a_entero"

:: Inicializar estados y estados de los procesos
for /L %%i in (0,1,7) do (
    set "status[%%i]= "
    set "estados[%%i]= "
)

:: Calcular longitud máxima de las opciones
set "max_length=0"
for /L %%i in (0,1,7) do (
    set "option=!options[%%i]!"
    call :length "!option!" option_length
    if !option_length! gtr !max_length! set "max_length=!option_length!"
)

:menu
cls
echo.
echo +===================================================================+
echo ^|                            MENU DEBUG                             ^|
echo +===================================================================+
echo ^| Seleccione un numero para alternar la seleccion de una opcion.    ^|
echo ^| Presione "Intro" sin seleccionar nada para finalizar la seleccion.^|
echo +-------------------------------------------------------------------+
echo ^| Ind   ^|    Estado   ^|     Tipo de depuracion       ^|    Modo      ^|
echo +-------------------------------------------------------------------+

:: Mostrar opciones dinámicamente
for /L %%i in (0,1,7) do (
    set "option=!options[%%i]!"
    call :format_option "!option!" formatted_option
    call :pad_right "!formatted_option!" !max_length! padded_option
    call :pad_right "[%%i]" 7 padded_index
    call :pad_right "!status[%%i]!" 11 padded_status
    call :pad_right "!estados[%%i]!" 11 padded_estados
    echo ^|!padded_index!^| !padded_status! ^| !padded_option! ^| !padded_estados!  ^|
)

echo +===================================================================+
echo ^| [8] Borrar selecciones                                            ^|
echo ^| [9] Presiona para salir                                           ^|
echo +===================================================================+
echo.

set /p choice="Elige una opcion del (0-8) o 9 para salir: "

rem Verificar si se presionó Enter sin elegir ninguna opción
if "%choice%"=="" (
    for /L %%i in (0,1,7) do (
        set "status[%%i]= "
        set "estados[%%i]= "
    )
    set "exit_code=0"
    goto end
) 

rem Verificar si se eligió la opción 8 para borrar las opciones seleccionadas
if "%choice%"=="8" (
    for /L %%i in (0,1,7) do (
        set "status[%%i]= "
        set "estados[%%i]= "
    )
    set "exit_code=1"
    goto menu
)

rem Verificar si se eligió la opción 9 para salir
if "%choice%"=="9" (
    goto show_selected
)

rem Verificar si la opción elegida es inválida
if "%choice%" lss "0" (
    echo Opcion invalida, intenta de nuevo.
    timeout /t 2 >nul
    set "exit_code=3"
    goto menu
) 

if "%choice%" gtr "8" (
    echo Opcion invalida, intenta de nuevo.
    timeout /t 2 >nul
    set "exit_code=3"
    goto menu
) 

:: Alternar el estado de la opción seleccionada
for /L %%i in (0,1,7) do (
    if "%choice%"=="%%i" (
        if "!status[%%i]!"==" " (
            set "status[%%i]=    >>>   "
            set "estados[%%i]=Pendiente"
            set "exit_code=4"
        ) else (
            set "status[%%i]= "
            set "estados[%%i]= "
            set "exit_code=5"
        )
    )
)

goto menu

:show_selected
cls
echo +===================================================================+
echo ^|                        PROCESOS SELECCIONADOS                     ^|
echo +===================================================================+
for /L %%i in (0,1,7) do (
    if "!status[%%i]!"=="    >>>   " (
        set "option=!options[%%i]!"
        echo !option!
    )
)
echo +===================================================================+

:: Guardar los procesos seleccionados en un archivo de texto
(
    echo Procesos seleccionados:
    for /L %%i in (0,1,7) do (
        if "!status[%%i]!"=="    >>>   " (
            set "option=!options[%%i]!"
            echo !option!
        )
    )
) > selectedProcesses.txt

pause
goto end

:end
exit /b 1

:replace_underscores
setlocal
set "_str=%~1"
set "_str=!_str:_= !"
echo %_str%
endlocal & set "option_with_spaces=%_str%"
exit /b

:format_option
setlocal
set "option=%~1"
set "option=%option:_= %"
set "option=%option:~0,1%%option:~1%"
endlocal & set "%~2=%option%"
exit /b

:length
setlocal
set "str=%~1"
set "len=0"
:lenloop
if not "%str%"=="" (
    set "str=%str:~1%"
    set /a len+=1
    goto lenloop
)
endlocal & set "%~2=%len%"
exit /b

:pad_right
setlocal
set "str=%~1"
set "target_len=%~2"
call :length "%str%" str_length
set /a pad_len=target_len - str_length
set "padding="
for /L %%i in (1,1,%pad_len%) do set "padding=!padding! "
endlocal & set "%~3=%str%%padding%"
exit /b