import pandas as pd
import redis
import random
import time
import grpc
import dns_service_pb2
import dns_service_pb2_grpc
import statistics

# Conectar a las instancias de Redis (particiones)
redis_instances = [
    redis.Redis(host='localhost', port=6379, db=0),
    redis.Redis(host='localhost', port=6380, db=0),
    redis.Redis(host='localhost', port=6381, db=0),
    redis.Redis(host='localhost', port=6382, db=0),
    redis.Redis(host='localhost', port=6383, db=0),
    redis.Redis(host='localhost', port=6384, db=0),
    redis.Redis(host='localhost', port=6385, db=0),
    redis.Redis(host='localhost', port=6386, db=0)  
]

numero_particiones = 4
contador_peticiones = 0
peticiones = [0] * 8
contador_hit = 0
contador_miss = 0
response_times = []  # Lista para almacenar los tiempos de respuesta
start_index = 0  
end_index = 20000 

# Conectar al servidor gRPC
def lookup_dns(domain):
    with grpc.insecure_channel('localhost:50052') as channel:
        stub = dns_service_pb2_grpc.DNSServiceStub(channel)
        response = stub.Lookup(dns_service_pb2.DNSRequest(domain=domain))
        return response.ip_address


# Función para obtener la partición correcta (instancia de Redis) basado en la clave
def get_redis_instance(key):
    first_char = key[0].lower()  
    
    # Asignar claves a diferentes instancias según rangos de letras/números
    if 'a' <= first_char <= 'g':  
        peticiones[0]+= 1
        return redis_instances[0]  
    elif 'h' <= first_char <= 'n':  
        peticiones[1]+= 1
        return redis_instances[1]  
    elif 'o' <= first_char <= 'u':  
        peticiones[2]+= 1
        return redis_instances[2]  
    elif 'h' <= first_char <= 'n':  
        peticiones[3]+= 1
        return redis_instances[3]  
    else:
        peticiones[0]+= 1
        return redis_instances[0]  


def generate_value(key):
    return f"Valor generado para {key}"


def cache_data(key):
    value = generate_value(key)  # Generar el valor para la clave
    instance = get_redis_instance(key)
    instance.set(key, value)


def load_csv_to_cache(file_path, max_lines=None):
    print("Leyendo archivo CSV...")
    df = pd.read_csv(file_path, header=None)  
    if max_lines is not None:
        df = df.head(max_lines)  # Limitar el número de líneas
    print("Archivo CSV leído. Cargando datos en caché...")
    for key in df[0]:  
        cache_data(key)  
    print("Datos cargados en caché")

# Función para recuperar datos desde la caché
def get_cached_data(key):
    instance = get_redis_instance(key)
    return instance.get(key)

# Leer el CSV para obtener las claves
def load_keys_from_csv(file_path):
    df = pd.read_csv(file_path, header=None)  
    print("hola mundo")
    return df[0].tolist()  # Devolver todas las claves

# Función para generar tráfico 
def generate_traffic(keys, num_requests):
    for _ in range(num_requests):  # Limitar el número de peticiones
        subset = keys[start_index:end_index]
        key = random.choice(subset)  # Seleccionar una clave aleatoria del CSV en el rango
        global contador_peticiones
        contador_peticiones+= 1
        print(f"CONTADOR PETICIONES = {contador_peticiones}")

        start_time = time.time()  
        
        value = get_cached_data(key)  # Hacer la petición a Redis
        if value is None:
            global contador_miss
            contador_miss += 1 
            value = lookup_dns(key)
            redis_instance = get_redis_instance(key)
            redis_instance.set(key, value)
            print(f"Cache miss looking up DNS for {key}: {value if value else 'No encontrado'}")
        else:
            global contador_hit
            contador_hit += 1
            print("HIT")
        
        end_time = time.time()  
        response_times.append(end_time - start_time)
        
 
csv_file_path = '3rd_lev_domains.csv'


load_csv_to_cache(csv_file_path,2000)
generate_traffic(load_keys_from_csv(csv_file_path),5000)
average_response_time = statistics.mean(response_times)
stddev_response_time = statistics.stdev(response_times)


print(f"LAS PETICIONES POR PARTICION SON {peticiones}")
print(f"CONTADOR HIT = {contador_hit}")
print(f"CONTADOR MISS = {contador_miss}")
print(f"PROMEDIO DE TIEMPO DE RESPUESTA = {average_response_time:.6f} segundos")
print(f"DESVIACIÓN ESTÁNDAR DE TIEMPO DE RESPUESTA = {stddev_response_time:.6f} segundos")