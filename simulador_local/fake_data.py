from faker import Faker
from random import randint, seed, getrandbits, sample,choice,randrange
from datetime import datetime,timedelta
from pandas import date_range
from bson import ObjectId

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
    
def get_pressure_timeline(sample_size,low_pressure,high_pressure):
    """Genera valores aleatorios para la linea de tiempo de presion.

    Args:
        sample_size (int): tamaño de la muestra 
        low_pressure (int): valor minimo de la presion
        max_pressure (int): valor maximo de la presion

    Returns:
        list (int): retorna una lista con los valores de profundidad
    """
    increment = (high_pressure-low_pressure)/(sample_size*0.75)
    decrement = (high_pressure-low_pressure)/(sample_size*0.25)
    try:
        pressure_values = [low_pressure]
        current_value = low_pressure
        for i in range(sample_size):
            if i>int(0.75*320):
                current_value-=decrement*choice([0.75,1,1.2])
                current_value = max(current_value,low_pressure)

            elif current_value>=high_pressure:
                current_value-=decrement*choice([1,1.5])
            else:
                current_value+= increment*choice([0.75,1,1.5])
            pressure_values.append(current_value)
        return pressure_values
    except Exception as e:
        print("ERROR: {e}")
        print("Hint: Error creando los valores de la presion")
        return []
   

def create_buzo_dl(data_size,buzos_ids,dataloggers_ids):
    buzo_dl = []
    buzos_ids = sample(buzos_ids,data_size)
    dataloggers_ids = sample(dataloggers_ids,data_size)
    for i in range(data_size):
        buzo_id = buzos_ids.pop(randint(0,len(buzos_ids)-1))
        dataloggers_id = dataloggers_ids.pop(randint(0,len(dataloggers_ids)-1))
        buzo_dl.append({"id_buzo":buzo_id,
                          "id_datalogger":dataloggers_id}
                        )
    return buzo_dl

def fake_datalogger(data_size):
    """Genera registros falsos para la tabla Datalogger en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        new_data = []
        for i in range(data_size):
            new_data.append(
                {
                "cod_datalogger": f'{randrange(16**8):08x}'
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Datalogger")
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
                "password": hex(getrandbits(32)),
                "company": fake.company()
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Supervisor")
        return []
    
def fake_buzo(data_size):
    """Genera registros falsos para la tabla Buzo en formato diccionario.

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
                "img":fake.image_url()
                #,id_team: lista de los equipos en los que ha estado/ultimo equipo que estuvo
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Buzo")
        return []   

def fake_operacion(data_size,supervisors_ids,buzos_ids,dataloggers_ids,magnitud):
    """Genera registros falsos para la tabla Faena en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.
        buzos_ids (list) = ids de los registros en Buzo agregados
        dataloggers_ids (list): ids de los registros en Dataloggers agregados
        magnitud (int): valor entero que representa la magnitud de los datos a generar.
                Se usa para aumentar o disminuir la ventana de tiempo para elegir
                un start_time y end_time

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:

        seed(datetime.now().timestamp())
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            start_time = fake.unix_time(datetime.today(),datetime.today() - timedelta(days=14*magnitud))
            end_time = datetime.fromtimestamp(start_time) + timedelta(hours=9)
            new_data.append(
                {
                "id_sup": supervisors_ids[randint(0,len(supervisors_ids)-1)],
                "list_buzo_dl": create_buzo_dl(5,buzos_ids,dataloggers_ids),
                "start_date":datetime.fromtimestamp(start_time),
                "end_date":end_time
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Operacion")
        return []
    
def fake_inmersion(data_size,operaciones_ids,operacion_table,dataloggers_ids):
    """Genera registros falsos para la tabla Team en formato diccionario.

    Args:
        data_size (int): cantidad de registros a generar.
        buzos_ids (list): lista con los id existentes de la tabla Buzo.

    Returns:
        list: lista de diccionarios con los registros nuevos.
    """
    try:
        seed(datetime.now().timestamp())
        length_in_minutes = 80
        fake = Faker("es_CL")
        new_data = []
        for i in range(data_size):
            id_operacion = operaciones_ids[randint(0,len(operaciones_ids)-1)]
            ope_start_date = operacion_table.find({"_id":{"$eq":id_operacion}},{"_id":0,"start_date":1})[0]['start_date']
            start_date = fake.unix_time(ope_start_date+timedelta(hours=8),
                                        ope_start_date)
            #end_date = start_date + timedelta(minutes=length_in_minutes)
            timeline_time = get_timeline(datetime.fromtimestamp(start_date),15,length_in_minutes)
            pressure_timeline = get_pressure_timeline(len(timeline_time),180,300)
            new_data.append(
                {
                #"id_buzo":buzos_ids[randint(0,len(buzos_ids)-1)],
                "id_datalogger":dataloggers_ids[randint(0,len(dataloggers_ids)-1)],
                "id_operacion":id_operacion,
                #"date":start_date,
                #"end_time": end_date,
                "presion": pressure_timeline,
                "time":timeline_time
                }
            )
        return new_data
    except Exception as e:
        print(f"ERROR: {e}")
        print("Hint: Error creando registros ficticion para Inmersion")
        return []