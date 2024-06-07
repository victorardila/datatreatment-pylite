## Funcionalidad
=============

### Objetivo
El objetivo de este proyecto es realizar la carga de datos en una base de datos columnares y documentales, para ello se debe realizar la limpieza de los datos y la carga de los mismos en la base de datos

### Descripcion
Estas es la especificacion del funcionamiento interno del proyecto

### Estructura de archivos automaticos
Este se basa en una arquitectura de 3 documentos csv al momento de la carga de datos, estos son:
- nombre_del_documento_generico.csv: Contiene la informacion entera de los datos a cargar
- nombre_del_documento_generico_clean.csv: Contiene la informacion de los datos limpios una vez que se ha realizado la depuracion de los datos
- nombre_del_documento_generico_sample.csv: Contiene una muestra de los datos para hacer pruebas de carga

### Carga de datos
El proceso de carga de datos se realiza de la siguiente manera:
1. Se carga el documento de datos generico se coloca en la carpeta de la ruta que usted haya definido
2. Se realiza la limpieza de los datos y se guarda en el documento de datos limpios
3. Se realiza la carga de los datos limpios en la base de datos

### Ejecucion de la carga de datos
Para realizar la carga de datos se debe ejecutar el siguiente comando:
```
python main.py
```
`Nota: En el archivo .env se debe definir: `
- `El tipo de servidor` de base de datos a la cual se va a realizar la carga de datos
- `La URI` de la base de datos (Instancia de mongoDB) a la cual se va a realizar la carga de datos
- `El Test` que es el valor que definira si se va a realizar la carga de datos en la base de datos de pruebas o en la base de datos de produccion

    