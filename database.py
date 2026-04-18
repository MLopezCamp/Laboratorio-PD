import redis


# Configuración de conexión al Redis local
# Se utiliza localhost porque el servicio de Redis corre en la misma maquina
r = redis.Redis(
    host='localhost', 
    port=6379, 
    db=0, 
    decode_responses=True,
    socket_timeout=2
)

def get_redis_client():
    """Retorna la instancia de conexión a Redis"""
    return r
