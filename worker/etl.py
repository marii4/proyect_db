from ssh_pymongo import MongoSession
import pandas as pd
import numpy as np

def get_data(session,table): #doc... datos nuevos
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
    if len(list(data))==0:
        return []
    new_data = []
    for document in data:
        buzo_ids = []
        for relation in document["list_buzo_dl"]:
            buzo_ids.append(relation["id_buzo"])
        document['list_id_buzo'] = buzo_ids
        new_data.append(document)
        new_data[-1].pop("list_buzo_dl")
    return new_data

def get_nitrogen(presion, tiempo):
    # Convertir la presión de atm a metros de agua
    profundidad = np.multiply(presion, 10)
    profundidad = np.subtract(profundidad, 10)
    # Calcular la descompresión en minutos
    descompresion = np.diff(tiempo) / 60
    descompresion = np.append(descompresion, descompresion[-1])
    # Calcular el nitrógeno acumulado en el tejido
    nitrogeno = np.cumsum(descompresion * profundidad)
    
    return profundidad, descompresion, nitrogeno

def get_inmersion_data(data,tabla_maestra):
    if len(list(data))==0:
        return []
    inmersion_df = pd.DataFrame(data)
    inmersion_table = pd.merge(inmersion_df,tabla_maestra, on=["id_operacion","id_datalogger"])[
            ["_id","id_operacion","id_buzo","id_datalogger","time","presion"]
        ].copy()
    pressure = inmersion_table["presion"].values
    prof = []
    desc = []
    nit = []
    for data in pressure:
        p,d,n = get_nitrogen(data,list(range(0,35*60,15)))
        prof.append(list(p))
        desc.append(list(d))
        nit.append(list(n))
    
    inmersion_table['nitrogen'] = nit
    inmersion_table['descomp'] = desc
    inmersion_table['prof'] = prof
    inmersion_table = inmersion_table.drop(['presion'],axis=1)
    return inmersion_table.to_dict('records') #cada fila a json

def get_buzo_data(local_session,buzo_table_name,cloud_session,inmersion_table_name):
    buzo_local_df = pd.DataFrame(local_session[buzo_table_name].find({}))
    #Saca toda la data historica de inmersion
    inmersion_data = cloud_session[inmersion_table_name].find({})
    inmersion_df = pd.DataFrame(inmersion_data)
    buzo_final_df = pd.merge(buzo_local_df,
         inmersion_df.groupby("id_buzo").agg(count=('id_buzo', 'count')),
         left_on="_id",right_on="id_buzo", how="left"
         ).rename(columns={"count":"n_inmersion"}).fillna(0)
    return buzo_final_df.to_dict('records')

def insert_buzo_data(session,table,data): #revisar
    if len(list(data))==0:
        return 0
    session[table].delete_many({})
    session[table].insert_many(data)
    session[table].update_many({}, {"$rename":{"exportDate":"insertDate"}})
    session[table].update_many({"insertDate":{"$eq":None}},{"$currentDate":{"insertDate":True}})
    return 1

