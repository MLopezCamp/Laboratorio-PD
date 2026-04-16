from fastapi import FastAPI, HTTPException
from database import Base, engine, SessionLocal
from models import Notificacion
from redis_client import redis_client

app = FastAPI()

Base.metadata.create_all(bind=engine)

@app.post("/notificar")
def notificar(data: dict):
    paciente_id = data["paciente_id"]
    cita_id = data["cita_id"]
    mensaje = data["mensaje"]

    lock_key = f"lock:notificacion:{paciente_id}:{cita_id}"
    key = f"notificacion:{paciente_id}:{cita_id}"

    # LOCK para concurrencia
    if not redis_client.set(lock_key, "1", nx=True, ex=5):
        raise HTTPException(status_code=400, detail="Proceso en curso")

    # Evitar duplicados
    if redis_client.get(key):
        raise HTTPException(status_code=400, detail="Notificación duplicada")

    # Guardar en DB
    db = SessionLocal()
    nueva = Notificacion(
        paciente_id=paciente_id,
        mensaje=mensaje,
        cita_id=cita_id
    )
    db.add(nueva)
    db.commit()
    db.close()

    # Marcar como enviada
    redis_client.set(key, "enviado", ex=60)

    print(f"Notificación enviada a paciente {paciente_id}")

    return {"mensaje": "Notificación enviada correctamente"}
