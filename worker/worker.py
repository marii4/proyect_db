from ssh_pymongo import MongoSession
import etl

local = MongoSession( #darle seguridad
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
#try: 
etl.insert_data(db_cloud,"Supervisor",etl.get_data(db_local,"Supervisor"))
etl.insert_data(db_cloud,"Datalogger",etl.get_data(db_local,"Datalogger"))

operacion_data = etl.get_data(db_local,"Operacion")
operacion_maestra = etl.operacion_maestra(operacion_data)
etl.insert_data(db_cloud,"Operacion",etl.get_operacion_data(operacion_data))

inmersion_data = etl.get_data(db_local,"Inmersion")
etl.insert_data(db_cloud,"Inmersion",etl.get_inmersion_data(inmersion_data,operacion_maestra))

buzo_data = etl.get_data(db_local,"Buzo")
etl.insert_buzo_data(db_cloud,"Buzo",etl.get_buzo_data(db_local,"Buzo",db_cloud,"Inmersion"))
#except Exception as e:
#    print(f'ERROR:{e}') #revisar
#    exit()
cloud.stop()
local.stop()