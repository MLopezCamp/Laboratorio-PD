import time
import random
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import httpx
app = FastAPI(title="Servicio de Notificaciones - Grupo 5")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
GRUPO1_URL = "http://172.16.0.244:8006"
class NotificacionRequest(BaseModel):
 cita_id: str
 paciente_id: str
 mensaje: str
 tipo: str = "email"
def simular_envio(tipo: str) -> bool:
 time.sleep(random.uniform(0.1, 0.5))
 return random.random() < 0.95
def obtener_cita(cita_id: str) -> dict:
 try:
 response = httpx.get(f"{GRUPO1_URL}/citas/{cita_id}", timeout=5)
 if response.status_code == 404:
 raise HTTPException(status_code=404, detail=f"Cita {cita_id} no existe")
 response.raise_for_status()
 return response.json()
 except httpx.ConnectError:
 raise HTTPException(status_code=503, detail="Servicio de citas no disponible")
@app.post("/notificar")
def notificar(req: NotificacionRequest):
 cita = obtener_cita(req.cita_id)
 if cita.get("paciente_id") != req.paciente_id:
 raise HTTPException(status_code=400, detail="El paciente no corresponde a la cita")
 dedup_key = f"notif:enviada:{req.cita_id}:{req.tipo}"
 if r.exists(dedup_key):
 raise HTTPException(status_code=409, detail="Notificación ya enviada para esta cita")
 lock_key = f"notif:lock:{req.cita_id}:{req.tipo}"
 lock = r.set(lock_key, "1", nx=True, ex=10)
 if not lock:
 raise HTTPException(status_code=423, detail="Notificación en proceso, intente de nuev try:
 exito = simular_envio(req.tipo)
 if not exito:
 raise HTTPException(status_code=502, detail="Fallo al enviar la notificación")
 timestamp = int(time.time())
 notif_key = f"notif:{req.cita_id}:{req.tipo}:{timestamp}"
 r.hset(notif_key, mapping={
 "cita_id": req.cita_id,
 "paciente_id": req.paciente_id,
 "doctor_id": cita.get("doctor_id", ""),
 "horario": cita.get("horario", ""),
 "estado_cita": cita.get("estado", ""),
 "mensaje": req.mensaje,
 "tipo": req.tipo,
 "timestamp": timestamp,
 "estado": "enviada"
 })
 r.set(dedup_key, notif_key, ex=86400)
 r.lpush("notif:lista", notif_key)
 return {
 "ok": True,
 "mensaje": f"Notificación '{req.tipo}' enviada para cita {req.cita_id}",
 "cita": {
 "cita_id": req.cita_id,
 "paciente_id": req.paciente_id,
 "doctor_id": cita.get("doctor_id"),
 "horario": cita.get("horario"),
 "estado": cita.get("estado")
 }
 }
 finally:
 r.delete(lock_key)
@app.post("/notificar_cita/{cita_id}")
def notificar_por_cita(cita_id: str, tipo: str = "email"):
 cita = obtener_cita(cita_id)
 req = NotificacionRequest(
 cita_id=cita_id,
 paciente_id=cita.get("paciente_id"),
 mensaje=f"Su cita con doctor {cita.get('doctor_id')} el {cita.get('horario')} está {c tipo=tipo
 )
 return notificar(req)
@app.get("/citas")
def obtener_citas():
 try:
 response = httpx.get(f"{GRUPO1_URL}/citas", timeout=5)
 response.raise_for_status()
 return response.json()
 except httpx.ConnectError:
 raise HTTPException(status_code=503, detail="Servicio de citas no disponible")
@app.get("/notificaciones")
def listar_notificaciones():
 keys = r.lrange("notif:lista", 0, -1)
 notificaciones = []
 for key in keys:
 data = r.hgetall(key)
 if data:
 notificaciones.append(data)
 return {"total": len(notificaciones), "notificaciones": notificaciones}
@app.get("/notificaciones/{cita_id}")
def notificaciones_por_cita(cita_id: str):
 keys = r.lrange("notif:lista", 0, -1)
 resultado = []
 for key in keys:
 data = r.hgetall(key)
 if data and data.get("cita_id") == cita_id:
 resultado.append(data)
 if not resultado:
 raise HTTPException(status_code=404, detail=f"No hay notificaciones para cita {cita_i return {"cita_id": cita_id, "notificaciones": resultado}
@app.get("/health")
def health():
 estado = {"status": "ok", "redis": "conectado", "grupo1": "desconocido"}
 try:
 r.ping()
 except Exception:
 estado["redis"] = "desconectado"
 estado["status"] = "degradado"
 try:
 response = httpx.get(f"{GRUPO1_URL}/health", timeout=3)
 estado["grupo1"] = "conectado" if response.status_code == 200 else "error"
 except httpx.ConnectError:
 estado["grupo1"] = "desconectado"
 estado["status"] = "degradado"
return estado
