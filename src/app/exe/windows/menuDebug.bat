@echo off
REM Script para abrir una ventana de selección de procesos de depuración de un dataframe
REM Autor: Victor Ardila

REM Declaración de opciones
set options=(
  "eliminar_filas_duplicadas"
  "eliminar_columnas_duplicadas"
  "eliminar_filas_nulas"
  "eliminar_columnas_nulas"
  "llenar_celdas_vacias"
  "quitar_caracteres_especiales"
  "formatear_fecha"
  "formatear_a_entero"
)

REM Inicialmente, ninguna opción está seleccionada
set selected_options=

REM Estado por defecto
set default_status=pendiente

REM Función para formatear el texto
:format_option
setlocal
set option=%~1
set "option=%option:_= %"  REM Reemplazar _ por espacios
set "option=%option:~0,1%%option:~1%"  REM Convertir la primera letra a mayúscula
echo %option%
endlocal
goto :eof

REM Función para verificar si una opción está seleccionada
:is_selected
setlocal
set option=%~1
for %%i in (%selected_options%) do (
  if "%%i"=="%option%" (
    endlocal
    exit /b 0
  )
)
endlocal
exit /b 1

REM Función para mostrar el menú de selección
:show_menu
echo MENU DEBUG
echo Seleccione un número para alternar la selección de una opción.
echo Presione <Intro> sin seleccionar nada para finalizar la selección.
echo.
echo Seleccionar        Tipo de depuración                    Estado
echo -----------------------------------------------------------------
for %%i in (%options%) do (
  call :format_option %%i
  if call :is_selected %%i (
    echo ✓ %%i            %option%            %default_status%
  ) else (
    echo   %%i            %option%
  )
)
echo.
goto :eof

REM Bucle para mostrar el menú de selección y alternar las opciones seleccionadas
:select_options
cls
call :show_menu
set /p "choice=Pulse <Intro> para finalizar la selección o pulse un número de selección: "

if "%choice%"=="" (
  REM Si el usuario presiona Enter sin seleccionar, finalizar la selección
  goto :eof
) else if "%choice%" geq "0" if "%choice%" lss "%options%" (
  REM Si el usuario selecciona una opción válida, alternar la selección de la opción
  set /a index=%choice%
  for /f "tokens=%index%" %%a in ("%options%") do set "option=%%a"
  call :is_selected %option%
  if errorlevel 1 (
    REM Si la opción no está seleccionada, seleccionarla
    set selected_options=%selected_options% %option%
  ) else (
    REM Si la opción ya está seleccionada, deseleccionarla
    set "new_selected_options="
    for %%i in (%selected_options%) do (
      if not "%%i"=="%option%" (
        set "new_selected_options=!new_selected_options! %%i"
      )
    )
    set "selected_options=%new_selected_options%"
  )
) else (
  REM Mensaje de error para entrada no válida
  echo Selección no válida, por favor intente de nuevo.
  timeout /t 1 /nobreak >nul
)
goto :select_options

REM Mostrar las opciones seleccionadas con el estado "pendiente"
echo Ha seleccionado las siguientes opciones:
for %%i in (%selected_options%) do (
  call :format_option %%i
  echo - %option% (Estado: %default_status%)
)

REM Devolver las opciones seleccionadas
echo %selected_options%
pause
