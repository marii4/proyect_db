from ssh_pymongo import MongoSession



def get_db_connection(server_ip,server_port,server_user,server_password,db_user,db_pass,db_port,db_name):

    try:
        server = MongoSession(
            host=server_ip,
            port=server_port,
            user=server_user,
            password=server_password,
            uri=f'mongodb://{db_user}:{db_pass}@127.0.0.1:{db_port}')
        
        db = server.connection[db_name]
        
        return db
    except Exception as e:
        print(f"ERROR: {e}")
        exit(-1)
    