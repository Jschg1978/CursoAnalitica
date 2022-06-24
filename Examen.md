# Examen Curso Analitica

## CONTENIDO

### Dado el siguiente modelo analítico.
 
El departamento de Marketing, desea enviar una campaña personalizada a cada uno de los clientes, promocionando el producto que más veces han solicitado. Por lo tanto el requerimiento ingresa al departamento de Ingeniería Analítica solicitado que se genere una tabla, con las siguientes columnas:

- Codigo del cliente (rowidcliente)
- Nombre del producto mas comprado
- Fecha de la última compra
- Correo del cliente

Las tablas del modelo analítico proveen los datos transaccionales. Sin embargo los correos electrónicos han sido proporcionados por medio de un archivo CSV el cual se encuentra adjunto.

### Lineamientos generales:

- El nombre de usuario debe ser la letra inicial del primer nombre y su apellido Ejm: Agustin Martinez (amartinez)
- Cada pipeline de ADF debe anteponer el nombre de usuario seguido de pipeline Ejm: amartinez_pipeline
- Los archivos ingestados en ADL2 deben colocarse en RAW en una carpeta con el nombre de usuario
- Los notebooks de Spark deben ser nombrados  {usuario}_notebook
- La tabla de resultados debe almacenarse en un POOL SQL, en el esquema default con el esquema "tbl_{usuario}"

Finalmente, cada participante deberá elaborar un archivo Markdown en Github con acceso publico. Por su puesto en el documento debe obviarse datos sensibles como claves etc.
El documento Markdown deberá contener el proceso técnico que el participante ha seguido con un orden lógico y el link del proyecto Githab deberá ser registrado como entregable de esta tarea.

### Modelo :


![image](https://user-images.githubusercontent.com/108036239/175609765-28faee4f-7110-4428-878a-78a493127f8b.png)

## EJECUCION

Para el desarrollo y ejecución de lo solicitado se procede de la siguiente manera.

### 1) Ingresar a Microsoft Azure

Con la cuenta y password se ingresa al portal de Azure

![image](https://user-images.githubusercontent.com/108036239/175611085-e1c07681-092e-458b-adba-59921b2d8d44.png)

### 2) Ingresar a Recursos

En los recursos asignados ingresarse a synapsecapacitacion

![image](https://user-images.githubusercontent.com/108036239/175611437-b38a2e66-ac4d-4e48-82fe-dc5d1e44a758.png)

### 3) Ingresar Synapse Studio

Dentro del area de trabajo de synapse, abrimos Synapse Studio para el desarrollo de la solución.

![image](https://user-images.githubusercontent.com/108036239/175611964-59a8d72b-41df-4e46-824b-cca0e2f34689.png)

### 4) Creación Servicio Vinculado

Para crear un Servicio Vinculado ejecutamos los siguientes pasos:

1) Conexiones Externas
2) Servicios Vinculados
3) Nuevo

![image](https://user-images.githubusercontent.com/108036239/175614845-f501d9c9-17f5-405f-9be2-497e17c8cd40.png)

4) Escogemos Mysql

![image](https://user-images.githubusercontent.com/108036239/175614493-282b47ef-7c7c-4e59-b185-3f7ef2be917c.png)

5) Para este caso vamos a utilizar el creado durante el curso

![image](https://user-images.githubusercontent.com/108036239/175615130-661aa61b-8f17-4a6b-9a25-5277485d1d96.png)

![image](https://user-images.githubusercontent.com/108036239/175616067-172550c1-a3b6-4357-9c83-fcf65a82602f.png)











