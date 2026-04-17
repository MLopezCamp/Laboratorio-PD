"""
GRUPO 6 - SERVICIO COORDINADOR (ORQUESTADOR)
Puerto: 8006
Arrancar: uvicorn main:app --host 0.0.0.0 --port 8006 --reload

Dependencias:
    pip install fastapi uvicorn httpx redis pydantic

Descripción:
    Este servicio actúa como orquestador del sistema distribuido de citas médicas.
    Coordina los servicios de Pacientes (G1), Doctores (G2), Citas (G3),
    Pagos (G4) y Notificaciones (G5) siguiendo un flujo secuencial con
    validaciones cruzadas y control de concurrencia via Redis.
"""

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis
import httpx
from typing import Optional
import logging

# ---------------------------------------------------------------------------
# Configuración básica de logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Servicio Coordinador - Grupo 6",
    description="Orquestador del sistema distribuido de citas médicas",
    version="2.0",
)

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ---------------------------------------------------------------------------
# URLs de los servicios externos
# Ajusta estas IPs/puertos según lo que cada grupo haya definido
# ---------------------------------------------------------------------------
PACIENTES_URL      = "http://172.16.0.193:8000"   # Grupo 1 - Pacientes
DOCTORES_URL       = "http://172.16.0.193:8000"   # Grupo 2 - Doctores
CITAS_URL          = "http://localhost:8003"   # Grupo 3 - Citas
PAGOS_URL          = "http://172.16.0.160:8004"   # Grupo 4 - Pagos
NOTIFICACIONES_URL = "http://localhost:8005"   # Grupo 5 - Notificaciones

TIMEOUT_CORTO  = 5   # segundos para GETs simples
TIMEOUT_NORMAL = 10  # segundos para POSTs normales
TIMEOUT_LARGO  = 20  # segundos para pagos (simulan delay)

# ---------------------------------------------------------------------------
# Modelos de entrada
# ---------------------------------------------------------------------------

class CrearPacienteRequest(BaseModel):
    nombre: str = Field(..., example="Ana Torres")
    email:  str = Field(..., example="ana@correo.com")


class OrquestacionRequest(BaseModel):
    paciente_id: int   = Field(..., example=1,       description="ID del paciente ya registrado")
    doctor_id:   int   = Field(..., example=3,       description="ID del doctor ya registrado")
    horario:     str   = Field(..., example="2025-07-10T10:00", description="Horario ISO 8601")
    monto:       float = Field(..., example=50000.0, description="Monto del pago en pesos")


# ---------------------------------------------------------------------------
# Helpers HTTP
# ---------------------------------------------------------------------------

async def http_get(client: httpx.AsyncClient, url: str, timeout: float = TIMEOUT_CORTO) -> dict:
    """GET genérico. Lanza HTTPException si el servicio falla."""
    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Timeout al consultar {url}")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"Servicio no disponible: {url}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"Error en servicio externo: {e.response.text}")


async def http_post(client: httpx.AsyncClient, url: str, payload: dict,
                    timeout: float = TIMEOUT_NORMAL) -> dict:
    """POST genérico. Lanza HTTPException si el servicio falla."""
    try:
        resp = await client.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Timeout al llamar {url}")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"Servicio no disponible: {url}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"Error en servicio externo ({url}): {e.response.text}")


# ---------------------------------------------------------------------------
# Endpoint: Proxy crear paciente (delega al Grupo 1)
# ---------------------------------------------------------------------------

@app.post("/crear_paciente", status_code=201, tags=["Pacientes"])
async def crear_paciente(datos: CrearPacienteRequest):
    """
    Proxy hacia el servicio de Pacientes (Grupo 1).
    Crea un nuevo paciente validando que no exista duplicado.
    """
    log.info(f"Creando paciente: {datos.nombre}")
    async with httpx.AsyncClient() as client:
        resultado = await http_post(
            client,
            f"{PACIENTES_URL}/crear_paciente",
            {"nombre": datos.nombre, "email": datos.email},
        )
    return resultado


# ---------------------------------------------------------------------------
# Endpoint: Listar pacientes (delega al Grupo 1)
# ---------------------------------------------------------------------------

@app.get("/pacientes", tags=["Pacientes"])
async def listar_pacientes():
    """
    Proxy hacia el servicio de Pacientes (Grupo 1).
    Devuelve la lista de todos los pacientes registrados.
    """
    async with httpx.AsyncClient() as client:
        resultado = await http_get(client, f"{PACIENTES_URL}/pacientes")
    return resultado


# ---------------------------------------------------------------------------
# Endpoint: Listar doctores (delega al Grupo 2)
# ---------------------------------------------------------------------------

@app.get("/doctores", tags=["Doctores"])
async def listar_doctores():
    """
    Proxy hacia el servicio de Doctores (Grupo 2).
    Devuelve la lista de todos los doctores disponibles.
    """
    async with httpx.AsyncClient() as client:
        resultado = await http_get(client, f"{DOCTORES_URL}/doctores")
    return resultado


# ---------------------------------------------------------------------------
# Endpoint: Listar citas (delega al Grupo 3)
# ---------------------------------------------------------------------------

@app.get("/citas", tags=["Citas"])
async def listar_citas():
    """
    Proxy hacia el servicio de Citas (Grupo 3).
    Devuelve todas las citas creadas en el sistema.
    """
    async with httpx.AsyncClient() as client:
        resultado = await http_get(client, f"{CITAS_URL}/citas")
    return resultado


# ---------------------------------------------------------------------------
# Endpoint principal: ORQUESTAR CITA COMPLETA
# ---------------------------------------------------------------------------

@app.post("/orquestar_cita", tags=["Orquestación"])
async def orquestar_cita(datos: OrquestacionRequest):
    """
    Flujo completo de creación de cita médica:

    1. Verificar que el paciente existe (Grupo 1)
    2. Verificar que el doctor existe y tiene disponibilidad (Grupo 2)
    3. Bloquear horario del doctor via Redis (anti-duplicidad)
    4. Crear la cita (Grupo 3) — valida que ni paciente ni doctor
       tengan otra cita en ese mismo horario
    5. Procesar el pago (Grupo 4) — con lock Redis anti-doble-pago
    6. Enviar notificación al paciente (Grupo 5) — idempotente
    7. Liberar el lock de orquestación

    Si cualquier paso falla, se reporta el error y se libera el lock.
    """

    # ------------------------------------------------------------------
    # LOCK de orquestación: evita que el mismo paciente-doctor-horario
    # se procese dos veces en paralelo
    # ------------------------------------------------------------------
    lock_key = f"lock:orquestacion:{datos.paciente_id}:{datos.doctor_id}:{datos.horario}"
    adquirido = r.set(lock_key, "procesando", nx=True, ex=90)

    if not adquirido:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Ya existe un proceso activo para el paciente {datos.paciente_id} "
                f"con el doctor {datos.doctor_id} en el horario {datos.horario}. "
                "Intenta de nuevo en unos segundos."
            ),
        )

    log.info(f"Lock adquirido: {lock_key}")
    resultado = {
        "paso_1_paciente":       None,
        "paso_2_doctor":         None,
        "paso_3_cita":           None,
        "paso_4_pago":           None,
        "paso_5_notificacion":   None,
        "errores":               [],
    }

    try:
        async with httpx.AsyncClient() as client:

            # ==============================================================
            # PASO 1 — Verificar que el paciente existe (Grupo 1)
            # ==============================================================
            log.info(f"[Paso 1] Verificando paciente id={datos.paciente_id}")
            try:
                paciente = await http_get(
                    client,
                    f"{PACIENTES_URL}/pacientes/{datos.paciente_id}",
                    timeout=TIMEOUT_CORTO,
                )
                resultado["paso_1_paciente"] = {
                    "id":     paciente.get("id"),
                    "nombre": paciente.get("nombre"),
                    "email":  paciente.get("email"),
                }
                log.info(f"[Paso 1] OK — paciente: {paciente.get('nombre')}")
            except HTTPException as e:
                # Si el paciente no existe, no tiene sentido continuar
                resultado["errores"].append(f"Paso 1 - Paciente: {e.detail}")
                raise HTTPException(
                    status_code=e.status_code,
                    detail=f"El paciente con id={datos.paciente_id} no existe o no está disponible: {e.detail}",
                )

            # ==============================================================
            # PASO 2 — Verificar que el doctor existe y bloquear su horario (Grupo 2)
            # ==============================================================
            log.info(f"[Paso 2] Verificando doctor id={datos.doctor_id} y bloqueando horario")
            try:
                bloqueo = await http_post(
                    client,
                    f"{DOCTORES_URL}/bloquear_horario",
                    {"doctor_id": datos.doctor_id, "horario": datos.horario},
                    timeout=TIMEOUT_NORMAL,
                )
                resultado["paso_2_doctor"] = bloqueo
                log.info(f"[Paso 2] OK — horario bloqueado: {bloqueo}")
            except HTTPException as e:
                # Si el doctor no existe o el horario ya está ocupado, abortamos
                resultado["errores"].append(f"Paso 2 - Doctor/Horario: {e.detail}")
                raise HTTPException(
                    status_code=e.status_code,
                    detail=f"No se pudo reservar el horario con el doctor {datos.doctor_id}: {e.detail}",
                )

            # ==============================================================
            # PASO 3 — Crear la cita (Grupo 3)
            # El servicio de citas valida internamente que no exista duplicidad
            # para ese paciente/doctor/horario, usando su propio lock Redis
            # ==============================================================
            log.info(f"[Paso 3] Creando cita")
            cita_id: Optional[int] = None
            try:
                cita = await http_post(
                    client,
                    f"{CITAS_URL}/crear_cita",
                    {
                        "paciente_id": datos.paciente_id,
                        "doctor_id":   datos.doctor_id,
                        "horario":     datos.horario,
                    },
                    timeout=TIMEOUT_NORMAL,
                )
                resultado["paso_3_cita"] = cita
                cita_id = cita.get("id")
                log.info(f"[Paso 3] OK — cita creada con id={cita_id}")
            except HTTPException as e:
                resultado["errores"].append(f"Paso 3 - Cita: {e.detail}")
                # Si la cita falla, liberamos el horario del doctor
                try:
                    await http_post(
                        client,
                        f"{DOCTORES_URL}/liberar_horario",
                        {"doctor_id": datos.doctor_id, "horario": datos.horario},
                        timeout=TIMEOUT_CORTO,
                    )
                    log.info("[Paso 3] Horario del doctor liberado por fallo en cita")
                except Exception:
                    log.warning("[Paso 3] No se pudo liberar el horario del doctor (rollback fallido)")
                raise HTTPException(
                    status_code=e.status_code,
                    detail=f"No se pudo crear la cita: {e.detail}",
                )

            # ==============================================================
            # PASO 4 — Procesar el pago (Grupo 4)
            # El servicio de pagos tiene su propio lock Redis anti-doble-pago
            # ==============================================================
            log.info(f"[Paso 4] Procesando pago para cita id={cita_id}")
            try:
                pago = await http_post(
                    client,
                    f"{PAGOS_URL}/pagar",
                    {
                        "cita_id":    cita_id,
                        "paciente_id": datos.paciente_id,
                        "monto":      datos.monto,
                    },
                    timeout=TIMEOUT_LARGO,   # pagos simulan delay
                )
                resultado["paso_4_pago"] = pago
                log.info(f"[Paso 4] OK — pago procesado: {pago}")
            except HTTPException as e:
                # Pago fallido: registramos el error pero NO cancelamos la cita
                # (la lógica de negocio puede decidir reintentarlo luego)
                resultado["errores"].append(f"Paso 4 - Pago: {e.detail}")
                log.warning(f"[Paso 4] Pago fallido para cita {cita_id}: {e.detail}")

            # ==============================================================
            # PASO 5 — Notificar al paciente (Grupo 5)
            # El servicio de notificaciones es idempotente (no duplica envíos)
            # ==============================================================
            log.info(f"[Paso 5] Enviando notificación para cita id={cita_id}")
            try:
                notif = await http_post(
                    client,
                    f"{NOTIFICACIONES_URL}/notificar",
                    {
                        "paciente_id": datos.paciente_id,
                        "tipo":        "cita_creada",
                        "cita_id":     cita_id,
                        "mensaje": (
                            f"Tu cita con el doctor {datos.doctor_id} "
                            f"para el {datos.horario} ha sido confirmada. "
                            f"Monto pagado: ${datos.monto:,.0f}."
                        ),
                    },
                    timeout=TIMEOUT_NORMAL,
                )
                resultado["paso_5_notificacion"] = notif
                log.info(f"[Paso 5] OK — notificación enviada")
            except HTTPException as e:
                # Notificación fallida: no es crítica, solo registramos
                resultado["errores"].append(f"Paso 5 - Notificación: {e.detail}")
                log.warning(f"[Paso 5] Notificación fallida: {e.detail}")

    finally:
        # Siempre liberamos el lock al terminar (éxito o excepción)
        r.delete(lock_key)
        log.info(f"Lock liberado: {lock_key}")

    # Determinar si el proceso fue exitoso (cita creada = éxito mínimo)
    exito = resultado["paso_3_cita"] is not None
    return {
        "exito":   exito,
        "mensaje": "Cita orquestada exitosamente" if exito else "Proceso fallido",
        "detalle": resultado,
    }


# ---------------------------------------------------------------------------
# Endpoint: Cancelar cita (delega al Grupo 3)
# ---------------------------------------------------------------------------

@app.delete("/cancelar_cita/{cita_id}", tags=["Citas"])
async def cancelar_cita(cita_id: int):
    """
    Proxy hacia el servicio de Citas (Grupo 3).
    Cancela una cita existente por su ID.
    Nota: también intenta liberar el horario del doctor si la cita existe.
    """
    async with httpx.AsyncClient() as client:

        # Primero obtenemos los datos de la cita para saber doctor/horario
        try:
            cita = await http_get(client, f"{CITAS_URL}/citas/{cita_id}")
        except HTTPException:
            cita = None  # Si no podemos obtenerla, igual intentamos cancelar

        # Cancelar en el servicio de citas
        resultado = await http_post(
            client,
            f"{CITAS_URL}/cancelar_cita",
            {"cita_id": cita_id},
        )

        # Si teníamos los datos, intentamos liberar el horario del doctor
        if cita and cita.get("doctor_id") and cita.get("horario"):
            try:
                await http_post(
                    client,
                    f"{DOCTORES_URL}/liberar_horario",
                    {"doctor_id": cita["doctor_id"], "horario": cita["horario"]},
                )
                log.info(f"Horario liberado para doctor {cita['doctor_id']}")
            except Exception:
                log.warning("No se pudo liberar el horario del doctor al cancelar la cita")

    return resultado


# ---------------------------------------------------------------------------
# Endpoint: Estado de todos los servicios
# ---------------------------------------------------------------------------

@app.get("/estado_servicios", tags=["Salud"])
async def estado_servicios():
    """
    Verifica el estado de todos los microservicios del sistema.
    Útil para diagnóstico rápido.
    """
    servicios = {
        "pacientes":      f"{PACIENTES_URL}",
        "doctores":       f"{DOCTORES_URL}",
        "citas":          f"{CITAS_URL}",
        "pagos":          f"{PAGOS_URL}/health",
        "notificaciones": f"{NOTIFICACIONES_URL}",
    }
    estados = {}

    async with httpx.AsyncClient() as client:
        for nombre, url in servicios.items():
            try:
                resp = await client.get(url, timeout=3)
                estados[nombre] = "ok" if resp.status_code == 200 else f"error ({resp.status_code})"
            except httpx.ConnectError:
                estados[nombre] = "no disponible"
            except httpx.TimeoutException:
                estados[nombre] = "timeout"

    # Estado de Redis
    try:
        r.ping()
        redis_estado = "ok"
    except Exception:
        redis_estado = "no disponible"

    todos_ok = all(v == "ok" for v in estados.values()) and redis_estado == "ok"

    return {
        "coordinador": "ok",
        "redis":        redis_estado,
        "servicios":    estados,
        "sistema_ok":   todos_ok,
    }


# ---------------------------------------------------------------------------
# Health check del propio orquestador
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Salud"])
def health():
    """Verifica que el servicio coordinador está activo."""
    return {"status": "ok", "servicio": "coordinador", "version": "2.0"}