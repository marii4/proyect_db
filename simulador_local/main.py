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
        uri='mongodb://pythonuser:pythonuser@127.0.0.1:27018')

    db = session.connection['db-local']

    #print(db.collection_names())

    buzo = db["Buzo"]
    datalogger = db["Datalogger"]
    supervisor = db["Supervisor"]
    operacion = db["Operacion"]
    inmersion = db["Inmersion"]
    

    #magnitud = pd.read_excel(args.magnitud_source_file,sheet_name=args.magnitud_sheet_name,index_col=0)
    #magnitud.columns = magnitud.columns.map(str) #Se asegura que las cols sean str

    m = args.magnitud
    m=4

    try:
        tabla= "Supervisor"
        data_supervisor = fd.fake_supervisor(5)
        supervisor.insert_many(data_supervisor)
        
        tabla= "Buzo"
        data_buzo = fd.fake_buzo(15)
        buzo.insert_many(data_buzo)
        
        tabla= "Datalogger"
        data_datalogger = fd.fake_datalogger(20)
        datalogger.insert_many(data_datalogger)
        
        tabla= "Operacion"
        data_operacion = fd.fake_operacion(5,fd.extract_ids(supervisor),fd.extract_ids(buzo),fd.extract_ids(datalogger),m)
        operacion.insert_many(data_operacion)

        tabla= "Inmersion"
        data_inmersion = fd.fake_inmersion(5*m,fd.extract_ids(operacion),operacion,fd.extract_ids(datalogger))
        inmersion.insert_many(data_inmersion)

    except Exception as e:
        print(f"ERROR: {e}")
        print(f"Hint: Problema agregando en la tabla {tabla}")
        exit(1)
    session.stop()
    #Para revision solamente
    if args.flag_output:
        print(f"Tamaño Supervisor: {len(data_supervisor)}")
        print(f"Tamaño Buzo: {len(data_buzo)}")
        print(f"Tamaño Datalogger: {len(data_datalogger)}")
        print(f"Tamaño Inmersion: {len(data_inmersion)}")
        print(f"Tamaño Operacion: {len(data_operacion)}")
        current_date = str(datetime.now().strftime('%d_%m_%Y'))
        try:
            os.mkdir(f"resources/salidas/{current_date}")
            print(f"Directorio {current_date} creado...")
            print("Guardando archivos...")
        except:
            print(f"Directorio {current_date} ya existe...\nGuardando archivos...")
        pd.DataFrame(data_supervisor).to_excel(f"resources/salidas/{current_date}/Supervisor_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_buzo).to_excel(f"resources/salidas/{current_date}/Buzo_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_datalogger).to_excel(f"resources/salidas/{current_date}/Datalogger_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_operacion).to_excel(f"resources/salidas/{current_date}/Datalogger_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        pd.DataFrame(data_inmersion).to_excel(f"resources/salidas/{current_date}/Inmersion_{datetime.now().strftime('%H_%M')}.xlsx",index=False)
        
    return 0    


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m','--magnitud', type = int, default=1,help="Magnitud de la generacion de datos")
    parser.add_argument('-msf','--magnitud_source_file',type=str, default=f"{PATH_TO_RESOURCES_MAGNITUD}", help="Path archivo de magnitud")
    parser.add_argument('-msn','--magnitud_sheet_name',type=str, default=f"{SHEET_NAME}", help="Nombre de la pagina donde esta la data")
    parser.add_argument('-fo','--flag_output',type=bool, default=True, help="Flag para exportar los datos generados")
    args = parser.parse_args()
    main(args)