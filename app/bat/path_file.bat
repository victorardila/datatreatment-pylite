@REM Script para abrir una ventana de selección de archivos y obtener la ruta del archivo seleccionado
@REM Autor: Victor Ardila

@echo off
setlocal enabledelayedexpansion

REM Comando para abrir una ventana de selección de archivos usando PowerShell
for /f "delims=" %%I in ('powershell -Command "$file = (New-Object -ComObject 'Shell.Application').BrowseForFolder(0, 'Seleccionar archivo', 0, 0).Self.Path; Write-Output $file"') do (
    set "archivo_seleccionado=%%~I"
)

REM Verificar si se seleccionó un archivo
if "%archivo_seleccionado%" == "" (
    echo false
) else (
    echo %archivo_seleccionado%
)

