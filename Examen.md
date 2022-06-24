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

# EJECUCION

Para el desarrollo y ejecución de lo solicitado se procede de la siguiente manera.

## 1) Ingresar a Microsoft Azure

Con la cuenta y password se ingresa al portal de Azure

![image](https://user-images.githubusercontent.com/108036239/175611085-e1c07681-092e-458b-adba-59921b2d8d44.png)

## 2) Ingresar a Recursos

En los recursos asignados ingresarse a synapsecapacitacion

![image](https://user-images.githubusercontent.com/108036239/175611437-b38a2e66-ac4d-4e48-82fe-dc5d1e44a758.png)

## 3) Ingresar Synapse Studio

Dentro del area de trabajo de synapse, abrimos Synapse Studio para el desarrollo de la solución.

![image](https://user-images.githubusercontent.com/108036239/175611964-59a8d72b-41df-4e46-824b-cca0e2f34689.png)

## 4) Creación Servicio Vinculado

Para crear un Servicio Vinculado ejecutamos los siguientes pasos:

a) Conexiones Externas
b) Servicios Vinculados
c) Nuevo

![image](https://user-images.githubusercontent.com/108036239/175614845-f501d9c9-17f5-405f-9be2-497e17c8cd40.png)

d) Escogemos Mysql

![image](https://user-images.githubusercontent.com/108036239/175614493-282b47ef-7c7c-4e59-b185-3f7ef2be917c.png)

d) Para este caso vamos a utilizar el creado durante el curso, previamente habiamos instalado un Integration Runtime en la máquina o server que tenga acceso a la base de datos y configuramos con el Key de Azure.

![image](https://user-images.githubusercontent.com/108036239/175615130-661aa61b-8f17-4a6b-9a25-5277485d1d96.png)

![image](https://user-images.githubusercontent.com/108036239/175616067-172550c1-a3b6-4357-9c83-fcf65a82602f.png)

e) Hacemos la prueba de la conexión

![image](https://user-images.githubusercontent.com/108036239/175616383-d301bd21-c9e3-4100-819a-102d2f7ce2a3.png)

## 5) Creación Data Lake Storage

Para crear un Servicio Vinculado ejecutamos los siguientes pasos:

a) Conexiones Externas
b) Servicios Vinculados
c) Nuevo

![image](https://user-images.githubusercontent.com/108036239/175614845-f501d9c9-17f5-405f-9be2-497e17c8cd40.png)

d) Escogemos Azure Data Lake Storage Gen2

![image](https://user-images.githubusercontent.com/108036239/175644478-17574c75-4a55-4c40-ad76-32b0080fe93b.png)

e) En este caso vamos a utilizar uno previamente creado en el curso.

![image](https://user-images.githubusercontent.com/108036239/175644587-d605dc1d-3c77-48c2-baac-62a18c962306.png)

f) Probamos conexión.

![image](https://user-images.githubusercontent.com/108036239/175644660-52ef7df6-7ff7-4675-8144-4163d230cbee.png)

## 6) Creación Origen de Datos 

a) Escogemos la opción datos

b) Vinculado

c) Conjunto de datos de integración 

![image](https://user-images.githubusercontent.com/108036239/175666002-0ae49f8d-1182-428d-8adb-3a4c379407b7.png)

![image](https://user-images.githubusercontent.com/108036239/175697652-f53854b3-8729-4831-9428-7e5fa0c37e79.png)

![image](https://user-images.githubusercontent.com/108036239/175697725-d67680b8-67dd-43a0-89a0-a819561a72d5.png)

d) En este caso utilizaremos una creada previamente en el curso (SourceDataset_xer)

![image](https://user-images.githubusercontent.com/108036239/175697875-191f1850-4362-4a3f-8447-72c2978d3c15.png)

![image](https://user-images.githubusercontent.com/108036239/175697924-1b56b120-7e64-4149-a95b-2ac20d12eeeb.png)

## 7) Creación Destino de Datos

a) Escogemos la opción datos

b) Vinculado

c) Conjunto de datos de integración 

![image](https://user-images.githubusercontent.com/108036239/175666002-0ae49f8d-1182-428d-8adb-3a4c379407b7.png)

![image](https://user-images.githubusercontent.com/108036239/175698823-fb711b5a-d390-44f8-899f-85ee76129a90.png)

d) Seleccionamos el formato

![image](https://user-images.githubusercontent.com/108036239/175698893-a72bd8c2-c8cd-46b7-834c-a87c6d3c8c2a.png)

e) Configuramos 

![image](https://user-images.githubusercontent.com/108036239/175699013-213f26e6-5c5e-4068-a398-3888a7f9ef34.png)

f) En este caso utilizaremos una creada para el examen (DestinationDataset_examen) 

![image](https://user-images.githubusercontent.com/108036239/175718588-813b42ea-a4d5-4ce4-99ab-a21850bc4ca4.png)

![image](https://user-images.githubusercontent.com/108036239/175719128-0d20ebc4-6106-46cc-8663-eb29c596cdb3.png)

## 8) Creación Pipeline

a) Creamos pipeline de acuerdo a los lineamientos generales

b) Canalizaciones - Nueva Canalización

![image](https://user-images.githubusercontent.com/108036239/175719429-27067369-26b9-483f-9af5-cf789e376fcd.png)

![image](https://user-images.githubusercontent.com/108036239/175719498-c8a30dd4-f7c9-41e8-96fd-00d065051b27.png)

c) En la solución planteada se incluye lo siguiente :

   1) Busqueda
   2) ForEach
   3) Notebook

![image](https://user-images.githubusercontent.com/108036239/175719692-1f887985-f9f5-4f28-b362-4867ede6d65a.png)

### Busqueda

![image](https://user-images.githubusercontent.com/108036239/175720001-6f32322c-3cfd-4556-bec1-b897500c5d6c.png)

Se parametriza la pestañas General y Configuración

![image](https://user-images.githubusercontent.com/108036239/175720083-63c7c8f1-90d4-4271-902b-5a359a28948e.png)

![image](https://user-images.githubusercontent.com/108036239/175720451-08280474-ec3f-4c89-9f92-c25bbb2f23ed.png)

Este proceso nos sirve para validar las tablas solicitadas como parte del modelo

![image](https://user-images.githubusercontent.com/108036239/175720587-432896bb-8861-46d6-9170-d4eafcd7f4e1.png)

Codigo Consulta para traer las tablas 

```sql
select 
table_name as tabla
from information_schema. tables
where
table_schema = 'dwh'
and table_name in('cliente','factura','facturaproducto','producto')
```

### ForEach

![image](https://user-images.githubusercontent.com/108036239/175721094-8e44c318-bb8a-474b-b27e-9f41c664751c.png)

Se parametriza la pestaña configuración

![image](https://user-images.githubusercontent.com/108036239/175721133-d77adf7e-c873-44cc-902e-36ed03f0ac4b.png)

Dentro del ForEach incluimos la actividad Copiar datos la misma nos sirve para tomar las tablas de un origen y copiar en un destino.

![image](https://user-images.githubusercontent.com/108036239/175721192-6c5256cf-2ee1-4694-8e5e-79056e4410b3.png)

Se parametriza el origen y destino de acuerdo creados en el apartado 6 y7 

![image](https://user-images.githubusercontent.com/108036239/175721464-8b1547ec-b4c7-4f30-a2c1-5078a08a5853.png)

![image](https://user-images.githubusercontent.com/108036239/175721488-d13b6897-c1ba-4505-adf7-674fb2c74a4d.png)

Resultado del copiado de tablas

![image](https://user-images.githubusercontent.com/108036239/175741355-c0825a4b-1349-4d1a-b9b9-c400125a97c0.png)


### Notebook

En este caso se crearon dos para la carga archivo de clientes con correo electronico en formato parquet y el otro para crear la tabla con los campos de salida.

#### Notebook Cargar Mail

![image](https://user-images.githubusercontent.com/108036239/175723506-9cc24271-98f5-4036-be0f-ddb5c933ce92.png)

![image](https://user-images.githubusercontent.com/108036239/175745352-c175a4e5-c79d-49a8-ae37-cc2352489cee.png)


```python
%%pyspark
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import explode

## RutaArchivoCsvEmail

Vpath = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/clientes_correos.csv'

## If header exists uncomment line below
##, header=True

## LeerArchivoCSVConCabecera
df = spark.read.csv(Vpath,header= True)

display(df.limit(10))
df.printSchema()

## GuardarTablaFijaEsquemaDefault

df.write.mode("overwrite").saveAsTable("default.tbl_email_jchicaiza")

## GuardarArchivoTipoParquet

VpathEmail = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jchicaiza/email.parquet'

df.write.mode("overwrite").parquet(VpathEmail)
```

![image](https://user-images.githubusercontent.com/108036239/175736948-1ba76942-7be0-4c84-9081-cb3bb9226680.png)

#### Notebook Resultado Final

![image](https://user-images.githubusercontent.com/108036239/175745002-d8d74cf3-e732-4875-965a-8f5fee7f97cb.png)

![image](https://user-images.githubusercontent.com/108036239/175744786-df48a647-8251-48d3-af79-96d910088d0a.png)

![image](https://user-images.githubusercontent.com/108036239/175746839-db19d3b9-d1c5-4159-bb35-c392fb191b8b.png)

Se ejecuto el siguiente proceso:

1) Se asigna las rutas a variables


```python
%%pyspark
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import explode

## RutaArchivos

VPath_Cli = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jchicaiza/cliente.parquet'
VPath_Fact = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw//jchicaiza/factura.parquet'
VPath_FactProd = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jchicaiza/facturaproducto.parquet'
VPath_Prod = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jchicaiza/producto.parquet'
VPath_Email = 'abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jchicaiza/email.parquet'
```
2) Se asigna a DataFrames para traer los datos de los archivos Parquet

```python
## Asignar a DataFrames


dfCliente = spark.read.load(VPath_Cli, format='parquet')
dfFactura = spark.read.load(VPath_Fact, format='parquet')
dfFactProd = spark.read.load(VPath_FactProd, format='parquet')
dfProducto = spark.read.load(VPath_Prod, format='parquet')
dfEmail = spark.read.load(VPath_Email, format='parquet')
```
3) Se crean Tablas Temporales


```python
## Creación de Tablas Temporales para ejecutar la seleccción final


##Tablas Temporales##
dfCliente.createOrReplaceTempView("tbl_Cliente_JCH")
dfFactura.createOrReplaceTempView("tbl_Factura_JCH")
dfFactProd.createOrReplaceTempView("tbl_FacturaProducto_JCH")
dfProducto.createOrReplaceTempView("tbl_Producto_JCH")
dfEmail.createOrReplaceTempView("tbl_Email_JCH")
```

4) Se crea el Script SQL para la creación de la tabla Final



```python
##SQL
vSQL="""
SELECT cli.rowidcliente,fp.producto,mail.email,count(producto) as cantidad,max(fp.fecha) as fechacompra
from tbl_Cliente_JCH cli
INNER JOIN tbl_Factura_JCH fac ON cli.rowidcliente = fac.rowidcliente
INNER JOIN tbl_FacturaProducto_JCh fp ON fac.rowidfactura = fp.rowidfactura
INNER JOIN tbl_Email_JCH mail ON cli.rowidcliente = mail.rowidcliente
group by cli.rowidcliente,fp.producto,mail.email

order by 1
"""
dfResultado=spark.sql(vSQL)

dfResultado.createOrReplaceTempView("tbl_Resultado_JCH")

display(dfResultado.limit(100))

VSQL1 ="""
select rowidcliente,max(cantidad) as cantidad from tbl_Resultado_JCH
group by rowidcliente
"""

dfMaxCant= spark.sql(VSQL1)
##display(dfMaxCant.limit(100))
dfMaxCant.createOrReplaceTempView("tbl_MaxCant_JCH")

VSQL2 ="""
select res.rowidcliente,res.producto,res.fechacompra,res.email
from tbl_Resultado_JCH res
INNER JOIN tbl_MaxCant_JCH mc ON mc.rowidcliente = res.rowidcliente and mc.cantidad = res.cantidad

"""

dfResFinal =spark.sql(VSQL2)
display(dfResFinal.limit(1000))

##Consumo
dfResFinal.write.mode("overwrite").saveAsTable("default.tbl_jchicaiza_resfinal")
```
5) Se consulta el resultado de la tabla final en el esquema default

![image](https://user-images.githubusercontent.com/108036239/175748963-426a4a06-4941-4b48-a57d-6207a67c90a8.png)


![image](https://user-images.githubusercontent.com/108036239/175749070-e11bec1b-f84d-4008-ad5f-32b4e71412e6.png)
