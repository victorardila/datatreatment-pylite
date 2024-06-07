## Estructuras de Cassandra
==========================
### Creacion de keyspace y configuracion de nodos
```sql
CREATE KEYSPACE IF NOT EXISTS data_air_quality
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
```
### Uso de keyspace
```sql
USE data_air_quality;
```
### Creacion de tablas
```sql
CREATE TABLE IF NOT EXISTS fechas (
    Fecha timestamp,
    PRIMARY KEY (Fecha)
);

CREATE TABLE IF NOT EXISTS mediciones (
    Tipo_de_estacion text,
    Tiempo_de_exposicion int,
    Variable text,
    Unidades text,
    Concentracion float,
    PRIMARY KEY (Tipo_de_estacion, Tiempo_de_exposicion, Variable)
);

CREATE TABLE IF NOT EXISTS ubicaciones (
    Latitud float,
    Longitud float,
    Codigo_del_departamento int,
    Departamento text,
    Codigo_del_municipio int,
    Nombre_del_municipio text,
    Nueva_columna_georreferenciada text,
    PRIMARY KEY (Latitud, Longitud, Codigo_del_departamento, Codigo_del_municipio)
);

CREATE TABLE IF NOT EXISTS descripciones (
    Autoridad_Ambiental text,
    Nombre_de_la_estacion text,
    Tecnologia text,
    PRIMARY KEY (Autoridad_Ambiental, Nombre_de_la_estacion)
);
```
