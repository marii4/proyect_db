from faker import Faker
from random import randint, seed, getrandbits, choices,choice
from datetime import datetime,timedelta
from numpy import array
from pandas import DataFrame, date_range

def extract_ids(table):
    """Extrae los ids de la data entregada

    Args:
        table (pymongo.collection.Collection): Lista de diccionarios donde se encuentra la data

    Returns:
        list: Arreglo que trae la lista de los ids unicos
    """
    try:
        cursor = table.find({},{"_id":1})
        ids = []
        for document in cursor:
            ids.append(document["_id"])
        return ids
    except Exception as e:
        print(f"ERROR:{e}")
        print("Problema extrayendo los ids")
        return []
    
def get_timeline(start_time,interval,length):
    """Calcula una serie de tiempo a partir de un tiempo de inicio y el intervalo

    Args:
        start_time (datetime.datetime): Fecha de inicio de la serie de tiempo
        interval (int): Ventana en segundos del intervalo que se utilzara para las muestras
        length (int): Tiempo en min del ancho de la serie de tiempo

    Returns:
        list(str): retorna una lista con los valores en str de la serie de tiempo
    """
    try:
        end_time = start_time+timedelta(minutes=length)
        periods = (end_time-start_time).seconds/interval
        timeline = date_range(start_time, periods=periods, freq=str(interval)+'s')
        return list(timeline.strftime("%d/%m/%Y %H:%M:%S"))
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Problema calculando la serie de tiempo")
        return []
    
def get_prof_timeline(sample_size,top_prof):
    """Genera valores aleatorios para la linea de tiempo de profundidad.

    Args:
        sample_size (int): tamaño de la muestra 
        top_prof (int): valor maximo de la profundidad

    Returns:
        list (int): retorna una lista con los valores de profundidad
    """
    init_prof = 0
    timeline_prof = [0]
    
    try:
        for i in range(sample_size-1):
            #Que empiece a subir una vez pasado el 75% del tiempo de muestra
            if i>int(0.75*sample_size):
                init_prof-=randint(0,2)
                if init_prof<0:
                    init_prof=0
            #Mientras no llegue al 60% de la prof maxima, seguira bajando        
            elif init_prof<0.60*top_prof:
                init_prof+=randint(0,2)
            #Pasado el 60%, subira o bajara 1 [m]    
            else:
                init_prof+=1*choice([-1,1])
            timeline_prof.append(init_prof)
        return timeline_prof
    except Exception as e:
        print("ERROR: {e}")
        print("Hint: Error creando los valores de la profundidad")
        return []
   
def fake_supervisor(data_size):
    """Genera registros falsos para la tabla Supervisor en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            new_data.append(
                {
                "name_sup":fake.given_name(),
                "email": fake.safe_email(),
                "password": hex(getrandbits(128)),
                "company": fake.company()
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Supervisor")
        return []
    
def fake_buzo(data_size):
    """Genera registros falsos para la tabla Faena en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            new_data.append(
                {
                "name_buzo":fake.given_name(),
                "company":fake.company(),
                "imagen":fake.image_url()
                #,id_team: lista de los equipos en los que ha estado/ultimo equipo que estuvo
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Buzo")
        return []   

def fake_team(data_size,sups_ids,buzos_ids):
    """Genera registros falsos para la tabla Team en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.
        sups_ids (numpy.array): lista con los id existentes de la tabla Supervisor.
        buzos_ids (numpy.array): lista con los id existentes de la tabla Buzo.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        ids_buzo_list = []
        for id in choices(buzos_ids,k=randint(3,10)):
            ids_buzo_list.append(int(id))

        for i in range(data_size):
            new_data.append(
                {
                "id_sup":int(sups_ids[randint(0,len(sups_ids)-1)]),
                "ids_buzo_list":ids_buzo_list
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Team")
        return []

def fake_faena(data_size,ids_teams,magnitud):
    """Genera registros falsos para la tabla Faena en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.
        magnitud (int): valor entero que representa la magnitud de los datos a generar.
                        Se usa para aumentar o disminuir la ventana de tiempo para elegir
                        un start_time y end_time
        ids_teams (numpy.array): ids de los registros en Team agregados

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:

        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            start_time = fake.unix_time(datetime.today()- timedelta(days=7*magnitud),datetime.today() - timedelta(days=14*magnitud))
            end_time = fake.unix_time(datetime.fromtimestamp(start_time) + timedelta(days=14) ,datetime.fromtimestamp(start_time) + timedelta(days=7)  )
            new_data.append(
                {
                "id_team": int(ids_teams[randint(0,len(ids_teams)-1)]),
                "start_date":start_time,
                "end_date":end_time
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Faena")
        return []
    
def fake_inmersion(data_size,buzos_ids,magnitud):
    """Genera registros falsos para la tabla Team en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.
        buzos_ids (numpy.array): lista con los id existentes de la tabla Buzo.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            date = fake.unix_time(datetime.today(),datetime.today()- timedelta(days=14*magnitud))
            timeline_time = get_timeline(datetime.fromtimestamp(date),15,80)
            timeline_prof = get_prof_timeline(len(timeline_time),40)
            new_data.append(
                {
                "id_buzo":int(buzos_ids[randint(0,len(buzos_ids)-1)]),
                "date":date,
                "timeline_time":timeline_time,
                "timeline_presion":[],
                "timeline_nitro":[],
                "timeline_prof":timeline_prof
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Inmersion")
        return []


def fake_alarm(data_size,buzos_ids,inmersions_ids):
    """Genera registros falsos para la tabla Team en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.
        buzos_ids (numpy.array): lista con los id existentes de la tabla Buzo.
        inmersions_ids (numpy.array): lista con los id existentes de la tabla Inmersion.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            new_data.append(
                {
                "id_buzo":int(buzos_ids[randint(0,len(buzos_ids)-1)]),
                "id_inmersion":int(inmersions_ids[randint(0,len(inmersions_ids)-1)]),
                "type_alarm":f"Alarma {randint(1,5)}",
                "time_total":randint(20,40),
                "level_nitro":randint(40,100)/100,
                "level_prof":randint(30,50)
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Inmersion")
        return []