import pandas as pd
import redis
import hashlib
import random
import time

# Conectar a las instancias de Redis (particiones)
redis_instances = [
    redis.Redis(host='localhost', port=6379, db=0),
    redis.Redis(host='localhost', port=6380, db=0),
    redis.Redis(host='localhost', port=6381, db=0),
    redis.Redis(host='localhost', port=6382, db=0),
    redis.Redis(host='localhost', port=6383, db=0),
    redis.Redis(host='localhost', port=6384, db=0),
    redis.Redis(host='localhost', port=6385, db=0),
    redis.Redis(host='localhost', port=6386, db=0)  # Opcional, si agregas otra instancia
]

numero_particiones = int(input( "Ingrese numero de particiones: " ))
peticiones_1 = 0
peticiones_2 = 0
peticiones_3 = 0
peticiones_4 = 0
peticiones_5 = 0
peticiones_6 = 0
peticiones_7 = 0
peticiones_8 = 0



# Función para obtener la partición correcta (instancia de Redis) basado en la clave
def get_redis_instance(key):
    if numero_particiones != 1:
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        instance_index = hash_value % numero_particiones
        return redis_instances[instance_index]
    else:
        return redis_instances[0]

# Función para generar un valor dinámico para la clave
def generate_value(key):
    return f"Valor generado para {key}"

# Función para almacenar datos en la caché (con valores generados dinámicamente)
def cache_data(key):
    value = generate_value(key)  # Generar el valor para la clave
    instance = get_redis_instance(key)
    instance.set(key, value)

# Leer el archivo CSV (sin cabecera) y cargar las claves en la caché
def load_csv_to_cache(file_path, max_lines=None):
    print("Leyendo archivo CSV...")
    df = pd.read_csv(file_path, header=None)  # Leer el CSV sin cabecera
    if max_lines is not None:
        df = df.head(max_lines)  # Limitar el número de líneas
    print(df.head())  # Mostrar las primeras filas del DataFrame para depuración
    print("Archivo CSV leído. Cargando datos en caché...")
    for key in df[0]:  # Acceder a la primera columna por índice (0)
        cache_data(key)  # Almacenar la clave y su valor generado en la caché
    print("Datos cargados en caché")

# Función para recuperar datos desde la caché
def get_cached_data(key):
    instance = get_redis_instance(key)
    return instance.get(key)

# Leer el CSV para obtener las claves
def load_keys_from_csv(file_path):
    df = pd.read_csv(file_path, header=None)  # Leer el CSV sin cabecera
    print("hola mundo")
    return df[0].tolist()  # Devolver todas las claves de la primera columna

# Función para generar tráfico basado en las claves del dataset
def generate_traffic(keys, num_requests):
    for _ in range(num_requests):  # Limitar el número de peticiones
        key = random.choice(keys)  # Seleccionar una clave aleatoria del CSV
        value = get_cached_data(key)  # Hacer la petición a Redis
        print(f"Solicitando {key}: {value.decode('utf-8') if value else 'No encontrado'}")
        time.sleep(random.uniform(0.1, 0.2))  # Simula un tiempo entre peticiones
        
 
# Ruta al archivo CSV
csv_file_path = '3rd_lev_domains.csv'

# Cargar las claves en Redis con valores generados
load_csv_to_cache(csv_file_path,10000)
generate_traffic(load_keys_from_csv(csv_file_path),100)

# Generar tráfico con 5000 peticiones basadas en las claves del CSV
## generate_traffic(load_keys_from_csv(csv_file_path), 10)







