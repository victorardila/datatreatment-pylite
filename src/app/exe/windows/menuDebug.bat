@echo off
setlocal enabledelayedexpansion

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
    set "estados[%%i]=Pendiente"
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
echo MENU DEBUG
echo Seleccione un numero para alternar la seleccion de una opcion.
echo Presione <Intro> sin seleccionar nada para finalizar la seleccion.
echo.
echo  Index      Estado              Tipo de depuracion     Modo
echo --------------------------------------------------------------

:: Mostrar opciones dinámicamente
for /L %%i in (0,1,7) do (
    set "option=!options[%%i]!"
    call :format_option "!option!" formatted_option
    call :pad_right "!formatted_option!" !max_length! padded_option
    call :pad_right "[%%i]" 7 padded_index
    call :pad_right "!status[%%i]!" 11 padded_status
    echo !padded_index!  !padded_status!  !padded_option!  !estados[%%i]!
)

echo ===================================================
echo [8] Presiona para salir
echo ===================================================
echo.

set /p choice="Elige una opcion del (0-7) o 0 para salir: "

if "%choice%"=="8" goto end

if "%choice%" lss "0" (
    echo Opcion invalida, intenta de nuevo.
    timeout /t 2 >nul
    goto menu
) 

if "%choice%" gtr "7" (
    echo Opcion invalida, intenta de nuevo.
    timeout /t 2 >nul
    goto menu
) 

:: Alternar el estado de la opción seleccionada
for /L %%i in (0,1,7) do (
    if "%choice%"=="%%i" (
        if "!status[%%i]!"==" " (
            set "status[%%i]=>>>"
        ) else (
            set "status[%%i]= "
        )
        set "estados[%%i]=Completado"
    )
)

goto menu

:end
echo Adios!
pause >nul
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