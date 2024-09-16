import pandas as pd
import redis
import hashlib
import random
import time
import grpc
import dns_service_pb2 
import dns_service_pb2_grpc

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

numero_particiones = int(input("Ingrese numero de particiones: "))
peticiones = [0] * 8  # Contador de peticiones por partición
start_index = 0  # Índice inicial del rango
end_index = 1000000 # Índice final del rango (no inclusivo)

# Conectar al servidor gRPC
def lookup_dns(domain):
    with grpc.insecure_channel('localhost:50052') as channel:
        stub = dns_service_pb2_grpc.DNSServiceStub(channel)
        response = stub.Lookup(dns_service_pb2.DNSRequest(domain=domain))
        return response.ip_address

# Función para obtener la partición correcta (instancia de Redis) basado en la clave
def get_redis_instance(key):
    if numero_particiones != 1:
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        instance_index = hash_value % numero_particiones
        peticiones[instance_index] += 1  # Contar peticiones
        return redis_instances[instance_index]
    else:
        return redis_instances[0]

# Función para generar un valor dinámico para la clave
def generate_value(key):
    return f" HIT Valor generado para {key}"

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
    return df[0].tolist()  # Devolver todas las claves de la primera columna

# Función para generar tráfico basado en las claves del dataset
def generate_traffic(keys, num_requests):
    for _ in range(num_requests):  # Limitar el número de peticiones
        subset = keys[start_index:end_index]
        key = random.choice(subset)  # Seleccionar una clave aleatoria del CSV en el rango
        value = get_cached_data(key)  # Hacer la petición a Redis
        if value is None:
            # Valor no encontrado en cache, hacer consulta DNS
            print(f"Cache miss for {key}, looking up DNS...")
            value = lookup_dns(key)
            redis_instance = get_redis_instance(key)
            redis_instance.set(key, value)
        if isinstance(value, bytes):
            print(f"Solicitando {key}: {value.decode('utf-8') if value else 'No encontrado'}")
        else:
            print(f"Solicitando {key}: {value if value else 'No encontrado'}")

# Ruta al archivo CSV
csv_file_path = '3rd_lev_domains.csv'

# Cargar las claves en Redis con valores generados
load_csv_to_cache(csv_file_path, 1000)
generate_traffic(load_keys_from_csv(csv_file_path), 100000)
