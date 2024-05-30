@REM Script para iniciar el servidor de Cassandra
@REM Autor: Victor Ardila

@echo off
set "ruta_actual=%~dp0"
cd /d %ruta_actual%..\..
set "ruta_cassandra=%CD%\database\apache-cassandra-local\bin"
start cmd /k "cd /d %ruta_cassandra% && cassandra.bat"
