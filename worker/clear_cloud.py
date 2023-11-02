from ssh_pymongo import MongoSession

cloud = MongoSession(
    "200.1.17.171",
    port=21100,
    user='wildsense',
    password='Wildsense',
    uri='mongodb://pythonuser:pythonuser@127.0.0.1:27019')

db_cloud = cloud.connection['db-cloud']

for collection in db_cloud.list_collection_names():
    db_cloud[collection].delete_many({})

cloud.close()