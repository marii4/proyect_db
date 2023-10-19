from ssh_pymongo import MongoSession
import fake_data as fd
import pandas as pd
import argparse
from datetime import datetime
import os

#Se pueden cambiar mediante la linea de comando
PATH_TO_RESOURCES_MAGNITUD = "resources/Magnitud.xlsx"
SHEET_NAME = "MagnitudData"

def main(args):

    session = MongoSession(
        "200.1.17.171",
        port=21100,
        user='wildsense',
        password='Wildsense',
        uri='mongodb://pythonuser:pythonuser@127.0.0.1:27019')

    db = session.connection['db-cloud']

    #print(db.collection_names())

    buzo = db["Buzo"]
    team = db["Team"]
    faena = db["Faena"]
    supervisor = db["Supervisor"]
    inmersion = db["Inmersion"]
    alarm = db["Alarm"]

    magnitud = pd.read_excel(args.magnitud_source_file,sheet_name=args.magnitud_sheet_name,index_col=0)
    magnitud.columns = magnitud.columns.map(str) #Se asegura que las cols sean str

    m = args.magnitud
    
    data_supervisor = fd.fake_supervisor(int(magnitud.loc['Supervisor',str(m)]))
    data_buzo = fd.fake_buzo(int(magnitud.loc['Buzo',str(m)]))
    data_team = fd.fake_team(int(magnitud.loc['Team',str(m)]),fd.extract_ids(data_supervisor),fd.extract_ids(data_buzo))
    data_faena = fd.fake_faena(int(magnitud.loc['Faena',str(m)]),fd.extract_ids(data_team),m)
    data_inmersion = fd.fake_inmersion(int(magnitud.loc['Inmersion',str(m)]),fd.extract_ids(data_buzo),m)
    data_alarm = fd.fake_alarm(20,fd.extract_ids(data_buzo),fd.extract_ids(data_inmersion))

    try:
        tabla= "Buzo"
        buzo.insert_many(data_buzo)
        tabla= "Team"
        team.insert_many(data_team)
        tabla= "Faena"
        faena.insert_many(data_faena)
        tabla= "Supervisor"
        supervisor.insert_many(data_supervisor)
        tabla= "Inmersion"
        inmersion.insert_many(data_inmersion)
        tabla= "Alarm"
        alarm.insert_many(data_alarm)
    except Exception as e:
        print(f"ERROR: {e}")
        print(f"Hint: Problema agregando en la tabla {tabla}")
    session.stop()
    #Para revision solamente
    if args.flag_output:
        print(f"Tamaño faena: {len(data_faena)}")
        print(f"Tamaño team: {len(data_team)}")
        print(f"Tamaño supervisor: {len(data_supervisor)}")
        print(f"Tamaño buzo: {len(data_buzo)}")
        print(f"Tamaño inmersion: {len(data_inmersion)}")
        print(f"Tamaño alarm: {len(data_alarm)}")
        current_date = str(datetime.now().strftime('%d_%m_%Y'))
        try:
            os.mkdir(f"resources/salidas/{current_date}")
            print(f"Directorio {current_date} creado...")
            print("Guardando archivos...")
        except:
            print(f"Directorio {current_date} ya existe...\nGuardando archivos...")
        pd.DataFrame(data_faena).to_excel(f"resources/salidas/{current_date}/Faena_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_team).to_excel(f"resources/salidas/{current_date}/Team_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_supervisor).to_excel(f"resources/salidas/{current_date}/Supervisor_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_buzo).to_excel(f"resources/salidas/{current_date}/Buzo_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_inmersion).to_excel(f"resources/salidas/{current_date}/Inmersion_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_alarm).to_excel(f"resources/salidas/{current_date}/Alarm_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
    return 0    


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m','--magnitud', type = int, default=1,help="Magnitud de la generacion de datos")
    parser.add_argument('-msf','--magnitud_source_file',type=str, default=f"{PATH_TO_RESOURCES_MAGNITUD}", help="Path archivo de magnitud")
    parser.add_argument('-msn','--magnitud_sheet_name',type=str, default=f"{SHEET_NAME}", help="Nombre de la pagina donde esta la data")
    parser.add_argument('-fo','--flag_output',type=bool, default=True, help="Flag para exportar los datos generados")
    args = parser.parse_args()
    main(args)