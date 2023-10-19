from ssh_pymongo import MongoSession

session = MongoSession(
    "200.1.17.171",
    port=21100,
    user='wildsense',
    password='Wildsense',
    uri='mongodb://pythonuser:pythonuser@127.0.0.1:27019') #27018 para bd local y 27019 para bd cloud

db = session.connection['db-cloud'] #prueba de conexion

print(db.collection_names())

buzo = db["Buzo"]
team = db["Team"]
faena = db["Faena"]
supervisor = db["Supervisor"]
inmersion = db["Inmersion"]
alarm = db["Alarm"]


session.stop()