"""
GRUPO 6 - SERVICIO COORDINADOR (ORQUESTADOR)
Puerto: 8006
Arrancar: uvicorn main:app --host 0.0.0.0 --port 8006 --reload

Dependencias:
    pip install fastapi uvicorn httpx redis pydantic

Descripción:
    Orquestador del sistema distribuido de citas médicas.
    Expone proxies hacia todos los servicios (G1–G5) y coordina
    el flujo completo de creación de una cita con control de
    concurrencia via Redis.

Endpoints disponibles:
    PACIENTES  (Grupo 1)  POST /crear_paciente          |  GET /pacientes  |  GET /pacientes/{id}
    DOCTORES   (Grupo 2)  POST /crear_doctor             |  GET /doctores   |  GET /disponibilidad/{id}
    CITAS      (Grupo 3)  GET  /citas  |  GET /citas/{id}|  DELETE /cancelar_cita/{id}
    PAGOS      (Grupo 4)  GET  /pagos  |  GET /pagos/{cita_id}
    ORQUESTA   (Grupo 6)  POST /orquestar_cita
    SALUD                 GET  /estado_servicios         |  GET /health
"""

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis
import httpx
from typing import Optional
import logging

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Servicio Coordinador - Grupo 6",
    description="Orquestador del sistema distribuido de citas médicas",
    version="3.0",
)

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ---------------------------------------------------------------------------
# URLs de los servicios externos
# Ajusta las IPs/puertos según lo que cada grupo defina en su máquina
# ---------------------------------------------------------------------------
PACIENTES_URL      = "http://172.16.0.253:9000"   # Grupo 1 - Pacientes
DOCTORES_URL       = "http://172.16.0.232:8003"   # Grupo 2 - Doctores
CITAS_URL          = "http://172.16.0.198:8001"   # Grupo 3 - Citas
PAGOS_URL          = "http://172.16.0.159:8004"   # Grupo 4 - Pagos
NOTIFICACIONES_URL = "http://localhost:8005"   # Grupo 5 - Notificaciones

TIMEOUT_CORTO  = 5    # GETs simples
TIMEOUT_NORMAL = 10   # POSTs normales
TIMEOUT_LARGO  = 20   # Pagos (simulan delay interno)

# ---------------------------------------------------------------------------
# Modelos de entrada
# ---------------------------------------------------------------------------

class CrearPacienteRequest(BaseModel):
    nombre: str = Field(..., example="Ana Torres")
    email:  str = Field(..., example="ana@correo.com")


class CrearDoctorRequest(BaseModel):
    nombre:       str = Field(..., example="Dr. Juan Pérez")
    especialidad: str = Field(..., example="Medicina General")
    email:        str = Field(..., example="dr.juan@hospital.com")


class OrquestacionRequest(BaseModel):
    paciente_id: str   = Field(..., example=1,                  description="ID del paciente ya registrado")
    doctor_id:   int   = Field(..., example=3,                  description="ID del doctor ya registrado")
    horario:     str   = Field(..., example="2025-07-10T10:00", description="Horario en formato ISO 8601")
    monto:       float = Field(..., example=50000.0,            description="Monto del pago en pesos")


# ---------------------------------------------------------------------------
# Helpers HTTP
# ---------------------------------------------------------------------------

async def http_get(client: httpx.AsyncClient, url: str, timeout: float = TIMEOUT_CORTO) -> dict:
    """GET genérico con manejo de errores unificado."""
    try:
        resp = await client.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Timeout al consultar: {url}")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"Servicio no disponible: {url}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"Error en servicio externo: {e.response.text}")


async def http_post(client: httpx.AsyncClient, url: str, payload: dict,
                    timeout: float = TIMEOUT_NORMAL) -> dict:
    """POST genérico con manejo de errores unificado."""
    try:
        resp = await client.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Timeout al llamar: {url}")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"Servicio no disponible: {url}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"Error en servicio externo ({url}): {e.response.text}")


async def http_delete(client: httpx.AsyncClient, url: str, timeout: float = TIMEOUT_NORMAL) -> dict:
    """DELETE genérico con manejo de errores unificado."""
    try:
        resp = await client.delete(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Timeout al llamar: {url}")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail=f"Servicio no disponible: {url}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code,
                            detail=f"Error en servicio externo ({url}): {e.response.text}")


# ===========================================================================
# GRUPO 1 — PACIENTES
# ===========================================================================

@app.post("/crear_paciente", status_code=201, tags=["Pacientes - Grupo 1"])
async def crear_paciente(datos: CrearPacienteRequest):
    """
    [Grupo 1] Crea un nuevo paciente en el sistema.
    Delega al servicio de Pacientes que valida duplicados internamente.
    """
    log.info(f"Creando paciente: {datos.nombre} | {datos.email}")
    async with httpx.AsyncClient() as client:
        return await http_post(
            client,
            f"{PACIENTES_URL}/pacientes/crear_paciente",
            {"nombre": datos.nombre, "email": datos.email},
        )


@app.get("/pacientes", tags=["Pacientes - Grupo 1"])
async def listar_pacientes():
    """
    [Grupo 1] Devuelve la lista de todos los pacientes registrados.
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{PACIENTES_URL}/pacientes/")


@app.get("/pacientes/{paciente_id}", tags=["Pacientes - Grupo 1"])
async def obtener_paciente(paciente_id: int):
    """
    [Grupo 1] Obtiene los datos de un paciente por su ID.
    Útil para verificar existencia antes de crear una cita.
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{PACIENTES_URL}/pacientes/{paciente_id}")


# ===========================================================================
# GRUPO 2 — DOCTORES
# ===========================================================================

@app.post("/crear_doctor", status_code=201, tags=["Doctores - Grupo 2"])
async def crear_doctor(datos: CrearDoctorRequest):
    """
    [Grupo 2] Registra un nuevo doctor en el sistema.
    Delega al servicio de Doctores que valida duplicados internamente.
    """
    log.info(f"Creando doctor: {datos.nombre} | {datos.especialidad}")
    async with httpx.AsyncClient() as client:
        return await http_post(
            client,
            f"{DOCTORES_URL}/crear_doctor",
            {
                "nombre":       datos.nombre,
                "especialidad": datos.especialidad,
                "email":        datos.email,
            },
        )


@app.get("/doctores", tags=["Doctores - Grupo 2"])
async def listar_doctores():
    """
    [Grupo 2] Devuelve la lista de todos los doctores registrados.
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{DOCTORES_URL}/doctores")


@app.get("/disponibilidad", tags=["Doctores - Grupo 2"])
async def disponibilidad_doctor(doctor_id: int, horario: str):
    """
    [Grupo 2] Consulta si un doctor tiene disponibilidad en un horario específico.
    Ejemplo: GET /disponibilidad?doctor_id=1&horario=8:30
    """
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{DOCTORES_URL}/disponibilidad",
                params={"doctor_id": doctor_id, "horario": horario},
                timeout=TIMEOUT_CORTO,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Timeout al consultar disponibilidad")
        except httpx.ConnectError:
            raise HTTPException(status_code=503, detail="Servicio de doctores no disponible")
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Error al consultar disponibilidad: {e.response.text}",
            )


# ===========================================================================
# GRUPO 3 — CITAS
# (La creación de citas se hace SOLO a través de /orquestar_cita)
# ===========================================================================

@app.get("/citas", tags=["Citas - Grupo 3"])
async def listar_citas():
    """
    [Grupo 3] Devuelve todas las citas del sistema.
    Para crear una cita usa POST /orquestar_cita (flujo completo).
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{CITAS_URL}/citas")


@app.get("/citas/{cita_id}", tags=["Citas - Grupo 3"])
async def obtener_cita(cita_id: int):
    """
    [Grupo 3] Obtiene los datos de una cita por su ID.
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{CITAS_URL}/citas/{cita_id}")


@app.delete("/cancelar_cita/{cita_id}", tags=["Citas - Grupo 3"])
async def cancelar_cita(cita_id: int):
    """
    [Grupo 3] Cancela una cita existente por su ID.
    También libera el horario del doctor como rollback (Grupo 2).
    """
    async with httpx.AsyncClient() as client:

        # Obtener datos de la cita para saber qué horario liberar
        try:
            cita = await http_get(client, f"{CITAS_URL}/citas/{cita_id}")
        except HTTPException:
            cita = None

        # Cancelar en el Grupo 3
        resultado = await http_delete(client, f"{CITAS_URL}/cancelar_cita/{cita_id}")

        # Rollback: liberar horario del doctor en el Grupo 2
        if cita and cita.get("doctor_id") and cita.get("horario"):
            try:
                await http_post(
                    client,
                    f"{DOCTORES_URL}/liberar_horario",
                    {"doctor_id": cita["doctor_id"], "horario": cita["horario"]},
                )
                log.info(f"[Cancelar] Horario liberado — doctor {cita['doctor_id']}")
            except Exception:
                log.warning("[Cancelar] No se pudo liberar el horario del doctor")

    return resultado


# ===========================================================================
# GRUPO 4 — PAGOS
# (El pago se ejecuta automáticamente dentro de /orquestar_cita)
# ===========================================================================

@app.get("/pagos", tags=["Pagos - Grupo 4"])
async def listar_pagos():
    """
    [Grupo 4] Devuelve el historial de todos los pagos procesados.
    El pago se genera automáticamente al usar POST /orquestar_cita.
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{PAGOS_URL}/pagos")


@app.get("/pagos/{cita_id}", tags=["Pagos - Grupo 4"])
async def obtener_pago(cita_id: str):
    """
    [Grupo 4] Consulta el pago asociado a una cita específica.
    """
    async with httpx.AsyncClient() as client:
        return await http_get(client, f"{PAGOS_URL}/pagos/cita/{cita_id}")


# ===========================================================================
# GRUPO 6 — ORQUESTACIÓN PRINCIPAL
# ===========================================================================

@app.post("/orquestar_cita", tags=["Orquestación - Grupo 6"])
async def orquestar_cita(datos: OrquestacionRequest):
    """
    Flujo completo y coordinado de creación de una cita médica:

      Paso 1 → Verificar que el paciente existe           (Grupo 1)
      Paso 2 → Verificar doctor y bloquear su horario     (Grupo 2)
      Paso 3 → Crear la cita                              (Grupo 3)
      Paso 4 → Procesar el pago                           (Grupo 4)
      Paso 5 → Notificar al paciente                      (Grupo 5)

    Control de concurrencia:
      - Lock Redis impide que el mismo paciente/doctor/horario
        se procese dos veces en paralelo (nx=True, expira en 90s).

    Rollback:
      - Si el Paso 3 falla → se libera el horario del doctor (Paso 2).
      - Si el Paso 4 o 5 fallan → se registran en 'errores' pero
        la cita permanece activa (fallos no críticos).
    """

    # ------------------------------------------------------------------
    # Lock de orquestación
    # ------------------------------------------------------------------
    lock_key = f"lock:orquestacion:{datos.paciente_id}:{datos.doctor_id}:{datos.horario}"
    adquirido = r.set(lock_key, "procesando", nx=True, ex=90)

    if not adquirido:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Ya hay un proceso activo para el paciente {datos.paciente_id} "
                f"con el doctor {datos.doctor_id} en el horario '{datos.horario}'. "
                "Espera unos segundos e intenta de nuevo."
            ),
        )

    log.info(f"[Lock] Adquirido: {lock_key}")

    resultado = {
        "paso_1_paciente":     None,
        "paso_2_doctor":       None,
        "paso_3_cita":         None,
        "paso_4_pago":         None,
        "paso_5_notificacion": None,
        "errores":             [],
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
                )
                resultado["paso_1_paciente"] = {
                    "id":     paciente.get("id"),
                    "nombre": paciente.get("nombre"),
                    "email":  paciente.get("email"),
                }
                log.info(f"[Paso 1] OK — {paciente.get('nombre')}")
            except HTTPException as e:
                resultado["errores"].append(f"Paso 1 - Paciente: {e.detail}")
                raise HTTPException(
                    status_code=e.status_code,
                    detail=f"Paciente id={datos.paciente_id} no encontrado: {e.detail}",
                )

            # ==============================================================
            # PASO 2 — Verificar doctor y bloquear horario (Grupo 2)
            # ==============================================================
            log.info(f"[Paso 2] Bloqueando horario del doctor id={datos.doctor_id}")
            try:
                bloqueo = await http_post(
                    client,
                    f"{DOCTORES_URL}/bloquear_horario",
                    {"doctor_id": datos.doctor_id, "horario": datos.horario},
                )
                resultado["paso_2_doctor"] = bloqueo
                log.info(f"[Paso 2] OK — horario bloqueado")
            except HTTPException as e:
                resultado["errores"].append(f"Paso 2 - Doctor/Horario: {e.detail}")
                raise HTTPException(
                    status_code=e.status_code,
                    detail=f"No se pudo reservar horario con doctor {datos.doctor_id}: {e.detail}",
                )

            # ==============================================================
            # PASO 3 — Crear la cita (Grupo 3)
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
                )
                resultado["paso_3_cita"] = cita
                cita_id = cita.get("id")
                log.info(f"[Paso 3] OK — cita id={cita_id}")
            except HTTPException as e:
                resultado["errores"].append(f"Paso 3 - Cita: {e.detail}")
                # Rollback: liberar el horario del doctor
                try:
                    await http_post(
                        client,
                        f"{DOCTORES_URL}/liberar_horario",
                        {"doctor_id": datos.doctor_id, "horario": datos.horario},
                        timeout=TIMEOUT_CORTO,
                    )
                    log.info("[Paso 3] Rollback OK: horario del doctor liberado")
                except Exception:
                    log.warning("[Paso 3] Rollback fallido: no se pudo liberar el horario")
                raise HTTPException(
                    status_code=e.status_code,
                    detail=f"No se pudo crear la cita: {e.detail}",
                )

            # ==============================================================
            # PASO 4 — Procesar el pago (Grupo 4) — fallo no crítico
            # ==============================================================
            log.info(f"[Paso 4] Procesando pago — cita={cita_id}, monto={datos.monto}")
            try:
                pago = await http_post(
                    client,
                    f"{PAGOS_URL}/pagar",
                    {
                        "cita_id":     cita_id,
                        "paciente_id": datos.paciente_id,
                        "monto":       datos.monto,
                    },
                    timeout=TIMEOUT_LARGO,
                )
                resultado["paso_4_pago"] = pago
                log.info(f"[Paso 4] OK — pago procesado")
            except HTTPException as e:
                resultado["errores"].append(f"Paso 4 - Pago: {e.detail}")
                log.warning(f"[Paso 4] Pago fallido (cita sigue activa): {e.detail}")

            # ==============================================================
            # PASO 5 — Notificar al paciente (Grupo 5) — fallo no crítico
            # ==============================================================
            log.info(f"[Paso 5] Notificando paciente id={datos.paciente_id}")
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
                            f"el {datos.horario} ha sido confirmada. "
                            f"Monto: ${datos.monto:,.0f}."
                        ),
                    },
                )
                resultado["paso_5_notificacion"] = notif
                log.info(f"[Paso 5] OK — notificación enviada")
            except HTTPException as e:
                resultado["errores"].append(f"Paso 5 - Notificación: {e.detail}")
                log.warning(f"[Paso 5] Notificación fallida: {e.detail}")

    finally:
        r.delete(lock_key)
        log.info(f"[Lock] Liberado: {lock_key}")

    exito = resultado["paso_3_cita"] is not None
    return {
        "exito":   exito,
        "mensaje": "Cita orquestada exitosamente" if exito else "Proceso fallido",
        "detalle": resultado,
    }


# ===========================================================================
# SALUD DEL SISTEMA
# ===========================================================================

@app.get("/estado_servicios", tags=["Salud"])
async def estado_servicios():
    """
    Verifica el estado (health) de todos los microservicios y de Redis.
    Útil para diagnóstico antes de ejecutar pruebas.
    """
    servicios = {
        "pacientes (G1)":      f"{PACIENTES_URL}",
        "doctores (G2)":       f"{DOCTORES_URL}",
        "citas (G3)":          f"{CITAS_URL}",
        "pagos (G4)":          f"{PAGOS_URL}/health",
        "notificaciones (G5)": f"{NOTIFICACIONES_URL}",
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


@app.get("/health", tags=["Salud"])
def health():
    """Health check del propio coordinador."""
    return {"status": "ok", "servicio": "coordinador", "version": "3.0"}