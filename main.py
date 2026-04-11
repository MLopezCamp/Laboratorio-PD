"""
GRUPO 6 – SERVICIO COORDINADOR (ORQUESTADOR)
Puerto sugerido: 8006
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import httpx
import asyncio

app = FastAPI(title="Servicio Coordinador", version="1.0")

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# ─── URLs de los demás servicios ──────────────────────────────────────────────
CITAS_URL          = "http://localhost:8001"
PACIENTES_URL      = "http://localhost:8002"
DOCTORES_URL       = "http://localhost:8003"
PAGOS_URL          = "http://localhost:8004"
NOTIFICACIONES_URL = "http://localhost:8005"
# ──────────────────────────────────────────────────────────────────────────────


class OrquestacionRequest(BaseModel):
    paciente_id: int
    doctor_id: int
    horario: str          # "2025-06-01T10:00"
    monto: float          # Valor de la cita


async def _post(client: httpx.AsyncClient, url: str, data: dict) -> dict:
    """Helper: POST con manejo de errores uniforme."""
    resp = await client.post(url, json=data, timeout=15)
    if resp.status_code not in (200, 201):
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Error en {url}: {resp.text}"
        )
    return resp.json()


async def _get(client: httpx.AsyncClient, url: str, params: dict = None) -> dict:
    """Helper: GET con manejo de errores uniforme."""
    resp = await client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


@app.post("/orquestar_cita")
async def orquestar_cita(datos: OrquestacionRequest):
    """
    Flujo completo de agendamiento de una cita médica:
    1. Valida que el paciente existe (Grupo 2)
    2. Bloquea el horario del doctor (Grupo 3)
    3. Crea la cita (Grupo 1)
    4. Procesa el pago (Grupo 4)
    5. Envía notificación (Grupo 5)

    Redis lock global evita que dos orquestaciones simultáneas
    para el mismo paciente+horario entren en conflicto.
    """
    lock_key = f"lock:orquestacion:{datos.paciente_id}:{datos.doctor_id}:{datos.horario}"
    adquirido = r.set(lock_key, "procesando", nx=True, ex=60)
    if not adquirido:
        raise HTTPException(
            status_code=409,
            detail="Ya hay una orquestación en proceso para este paciente/horario"
        )

    resultado = {}
    cita_creada = False

    try:
        async with httpx.AsyncClient() as client:

            # ── PASO 1: Verificar paciente ────────────────────────────────────
            try:
                paciente = await _get(
                    client,
                    f"{PACIENTES_URL}/pacientes/{datos.paciente_id}"
                )
                resultado["paciente"] = paciente
            except httpx.HTTPStatusError:
                raise HTTPException(
                    status_code=404,
                    detail=f"Paciente {datos.paciente_id} no encontrado"
                )

            # ── PASO 2: Bloquear horario del doctor ───────────────────────────
            bloqueo = await _post(
                client,
                f"{DOCTORES_URL}/bloquear_horario",
                {"doctor_id": datos.doctor_id, "horario": datos.horario}
            )
            resultado["horario_bloqueado"] = bloqueo

            # ── PASO 3: Crear cita ────────────────────────────────────────────
            cita = await _post(
                client,
                f"{CITAS_URL}/crear_cita",
                {
                    "paciente_id": datos.paciente_id,
                    "doctor_id": datos.doctor_id,
                    "horario": datos.horario
                }
            )
            cita_creada = True
            cita_id = cita.get("id", 0)   # Grupo 1 debe devolver id
            resultado["cita"] = cita

            # ── PASO 4: Procesar pago ─────────────────────────────────────────
            pago = await _post(
                client,
                f"{PAGOS_URL}/pagar",
                {
                    "cita_id": cita_id,
                    "paciente_id": datos.paciente_id,
                    "monto": datos.monto
                }
            )
            resultado["pago"] = pago

            # ── PASO 5: Enviar notificación ───────────────────────────────────
            notif = await _post(
                client,
                f"{NOTIFICACIONES_URL}/notificar",
                {
                    "paciente_id": datos.paciente_id,
                    "tipo": "cita_creada",
                    "cita_id": cita_id,
                    "mensaje": (
                        f"Tu cita con el doctor {datos.doctor_id} "
                        f"el {datos.horario} fue confirmada. "
                        f"Pago de ${datos.monto} procesado."
                    )
                }
            )
            resultado["notificacion"] = notif

    except HTTPException:
        # Si el horario fue bloqueado pero la cita falló, liberar el horario
        if not cita_creada and resultado.get("horario_bloqueado"):
            try:
                async with httpx.AsyncClient() as client:
                    await _post(
                        client,
                        f"{DOCTORES_URL}/liberar_horario",
                        {"doctor_id": datos.doctor_id, "horario": datos.horario}
                    )
            except Exception:
                pass   # Rollback best-effort
        raise
    finally:
        r.delete(lock_key)

    return {
        "mensaje": "Cita orquestada exitosamente",
        "detalle": resultado
    }


@app.get("/estado_servicios")
async def estado_servicios():
    """Verifica el health de todos los servicios del sistema."""
    servicios = {
        "citas":           f"{CITAS_URL}/health",
        "pacientes":       f"{PACIENTES_URL}/health",
        "doctores":        f"{DOCTORES_URL}/health",
        "pagos":           f"{PAGOS_URL}/health",
        "notificaciones":  f"{NOTIFICACIONES_URL}/health",
    }
    estados = {}
    async with httpx.AsyncClient() as client:
        tasks = {
            nombre: client.get(url, timeout=3)
            for nombre, url in servicios.items()
        }
        for nombre, task in tasks.items():
            try:
                resp = await task
                estados[nombre] = "ok" if resp.status_code == 200 else "error"
            except Exception:
                estados[nombre] = "no disponible"

    return {"coordinador": "ok", "servicios": estados}


@app.get("/health")
def health():
    return {"status": "ok", "servicio": "coordinador"}