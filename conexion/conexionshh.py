import paramiko
from pymongo import MongoClient
from paramiko.client import AutoAddPolicy


ssh_host = '200.1.17.171'  # Dirección del servidor Ubuntu
ssh_port = 21100  # Puerto SSH (por defecto es 22)
ssh_username = 'wildsense'  # Tu nombre de usuario en el servidor Ubuntu
ssh_password = 'Wildsense'  # Tu contraseña en el servidor Ubuntu

# Configura los detalles de la conexión MongoDB en el servidor Ubuntu
mongo_host = 'localhost'  # Cambia esto si MongoDB no está en localhost
mongo_port = 27018  # Puerto MongoDB (por defecto es 27017)
mongo_db = 'db-local'  # Nombre de tu base de datos
mongo_collection = 'Supervisor'  # Nombre de tu colección

# Establece una conexión SSH al servidor Ubuntu
ssh_client = paramiko.SSHClient()
ssh_client.load_system_host_keys()

ssh_client.connect(ssh_host, port=ssh_port, username=ssh_username, password=ssh_password)

# Establece una conexión a MongoDB a través de SSH
tunnel = ssh_client.get_transport().open_tunnel(mongo_host, mongo_port)
client = MongoClient(host=tunnel)
db = client[mongo_db]

# Realiza operaciones en la base de datos MongoDB
collection = db[mongo_collection]
documentos = collection.find()

for documento in documentos:
    print(documento)

# Cierra las conexiones

tunnel.close()
ssh_client.close()
