from ssh_pymongo import MongoSession
import pandas as pd
import etl

local = MongoSession(
    "200.1.17.171",
    port=21100,
    user='wildsense',
    password='Wildsense',
    uri='mongodb://pythonuser:pythonuser@127.0.0.1:27018')

cloud = MongoSession(
    "200.1.17.171",
    port=21100,
    user='wildsense',
    password='Wildsense',
    uri='mongodb://pythonuser:pythonuser@127.0.0.1:27019')

db_local = local.connection['db-local']
db_cloud = cloud.connection['db-cloud']

#print(f"Tablas DB Local: {db_local.collection_names()}")
#print(f"Tablas DB Cloud: {db_cloud.collection_names()}")

#Seccion de transformaciones
etl.insert_data(db_cloud,"Supervisor",etl.get_data(db_local,"Supervisor"))
etl.insert_data(db_cloud,"Datalogger",etl.get_data(db_local,"Datalogger"))

operacion_data = etl.get_data(db_local,"Operacion")
operacion_maestra = etl.operacion_maestra(operacion_data)
etl.insert_data(db_cloud,"Operacion",etl.get_operacion_data(operacion_data))

inmersion_data = etl.get_data(db_local,"Inmersion")
etl.insert_data(db_cloud,"Inmersion",etl.get_inmersion_data(inmersion_data,operacion_maestra))


#buzo_data = etl.get_data(db_local,"Buzo")
#supervisor_local_df = pd.DataFrame(supervisor_local.find({}))
#Convierte el dataframe trabajado en diccionario con to_dict y lo inserta en la BD cloud
#El parametro 'records' hace que el diccionario salga de la forma "columna: valor", que es el formato json
#https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
#https://stackoverflow.com/questions/20167194/insert-a-pandas-dataframe-into-mongodb-using-pymongo
#print(supervisor_local_df.to_dict('records'))
#supervisor_cloud.insert_many(supervisor_local_df.to_dict())


cloud.stop()
local.stop()