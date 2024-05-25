Basic python ETL

ETL muy sencillo utilizando el módulo petl de python.

El objetivo es cargar en un tabla de un base de datos MySQL el resumen de ventas de una empresa a partir de información disponible en varios archivos csv y xlsx.
Para ello vamos a utilizar SQLite como puente para las fases Extract y Transform del proceso ETL.

En la siguiente imagen podemos ver la estructura de la BBDD SQLite que se crea:

![image](https://github.com/jlanzosg/python-ETL/assets/170817631/5db24a4a-5557-41ad-b025-dcd6f4129c24)

En la siguiente imagen podemos ver el resultado de una consulta a la tabla creada en MySQL:

![image](https://github.com/jlanzosg/python-ETL/assets/170817631/fa3d6166-769f-45bf-b71f-fe94225c51e4)


