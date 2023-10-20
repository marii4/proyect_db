from ssh_pymongo import MongoSession
import pandas as pd


def get_data(session,table):
    cursor = session[table].find({"exportDate":{"$eq":None}})
    data = list(cursor)
    session[table].update_many({"exportDate":{"$eq":None}},{"$currentDate":{"exportDate":True}})
    return data 

def insert_data(session,table,data):
    if len(list(data))==0:
        return 0
    session[table].insert_many(data)
    session[table].update_many({"insertDate":{"$eq":None}},{"$currentDate":{"insertDate":True}})
    return 1

def operacion_maestra(data):
    if len(list(data))==0:
        return 0
    operacion_df = pd.DataFrame(data)
    operacion_maestra = operacion_df.join(
        operacion_df.explode("list_buzo_dl")["list_buzo_dl"].apply(pd.Series)
        ).copy()
    operacion_maestra = operacion_maestra.rename(columns={"_id":"id_operacion"})
    return operacion_maestra 

def get_operacion_data(data):
    new_data = []
    for document in data:
        buzo_ids = []
        for relation in document["list_buzo_dl"]:
            buzo_ids.append(relation["id_buzo"])
        document['list_id_buzo'] = buzo_ids
        new_data.append(document)
        new_data[-1].pop("list_buzo_dl")
    return new_data

def get_inmersion_data(data,tabla_maestra):
    inmersion_df = pd.DataFrame(data)
    inmersion_table = pd.merge(inmersion_df,tabla_maestra, on=["id_operacion","id_datalogger"])[
            ["_id","id_operacion","id_buzo","id_datalogger","time"]
        ]
    #Agregar logica de los campos faltantes nitro, descomp, etc etc
    return inmersion_table.to_dict('records')
