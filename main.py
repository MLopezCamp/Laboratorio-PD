from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import mysql.connector
import redis
import httpx
import uuid
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Servicio de Citas - Grupo 1", version="1.0.0")

# ─── Configuración ────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "user":     os.getenv("DB_USER",     "root"),
    "password": os.getenv("DB_PASSWORD", "root"),
    "database": os.getenv("DB_NAME",     "citas_db"),
}

REDIS_HOST       = os.getenv("REDIS_HOST",        "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT",    "6379"))
COORDINADOR_URL  = os.getenv("COORDINADOR_URL",   "http://172.18.18.6:8006")
LOCK_TTL_SECONDS = int(os.getenv("LOCK_TTL",      "10"))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    logger.info("Conexión a Redis establecida.")
except redis.RedisError as e:
    logger.error(f"No se pudo conectar a Redis: {e}")
    raise RuntimeError("Redis no disponible al iniciar el servicio.") from e


# ─── Helpers de BD ────────────────────────────────────────────────────────────

def get_db():
    """Devuelve una conexión fresca a MySQL."""
    return mysql.connector.connect(**DB_CONFIG)


def init_db():
    """Crea la tabla citas si no existe."""
    db = get_db()
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS citas (
            id          VARCHAR(36)  PRIMARY KEY,
            paciente_id INT          NOT NULL,
            doctor_id   INT          NOT NULL,
            horario     VARCHAR(50)  NOT NULL,
            estado      VARCHAR(20)  NOT NULL DEFAULT 'confirmada',
            creado_en   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.commit()
    cursor.close()
    db.close()
    logger.info("Tabla 'citas' verificada/creada.")


@app.on_event("startup")
def startup():
    init_db()


# ─── Modelos ──────────────────────────────────────────────────────────────────

class CitaRequest(BaseModel):
    paciente_id: int
    doctor_id:   int
    horario:     str          # e.g. "2025-06-10T09:00"


class CitaResponse(BaseModel):
    id:          str
    paciente_id: int
    doctor_id:   int
    horario:     str
    estado:      str
    creado_en:   Optional[str] = None


# ─── Integración con Coordinador ──────────────────────────────────────────────

async def notificar_coordinador(cita: dict) -> None:
    """
    Notifica al servicio coordinador (Grupo 6) de forma asíncrona.
    No bloquea la respuesta al cliente si el coordinador falla.
    """
    payload = {
        "origen":      "grupo1-citas",
        "evento":      "cita_creada",
        "cita_id":     cita["id"],
        "paciente_id": cita["paciente_id"],
        "doctor_id":   cita["doctor_id"],
        "horario":     cita["horario"],
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(f"{COORDINADOR_URL}/orquestar_cita", json=payload)
            resp.raise_for_status()
            logger.info(f"Coordinador notificado para cita {cita['id']}. Status: {resp.status_code}")
    except httpx.TimeoutException:
        logger.warning(f"Timeout al notificar coordinador para cita {cita['id']}.")
    except httpx.HTTPStatusError as e:
        logger.warning(f"Coordinador respondió con error {e.response.status_code} para cita {cita['id']}.")
    except Exception as e:
        logger.warning(f"Error inesperado al notificar coordinador: {e}")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    return {"servicio": "Citas Médicas", "grupo": 1, "status": "activo"}


@app.post("/crear_cita", response_model=CitaResponse, status_code=201)
async def crear_cita(cita: CitaRequest):
    """
    Crea una nueva cita médica.
    Usa lock distribuido en Redis para evitar condiciones de carrera cuando
    dos usuarios intentan reservar el mismo doctor en el mismo horario.
    """
    lock_key = f"lock:cita:doctor_{cita.doctor_id}:horario_{cita.horario}"
    lock_id  = str(uuid.uuid4())

    # ── Adquirir lock distribuido (SET NX EX) ──
    acquired = r.set(lock_key, lock_id, nx=True, ex=LOCK_TTL_SECONDS)
    if not acquired:
        raise HTTPException(
            status_code=409,
            detail=f"El horario '{cita.horario}' ya está siendo reservado para el doctor {cita.doctor_id}. Intenta en unos segundos."
        )

    db     = None
    cursor = None
    try:
        db     = get_db()
        cursor = db.cursor(dictionary=True)

        # ── Verificar duplicado en BD ──
        cursor.execute(
            """
            SELECT id FROM citas
            WHERE doctor_id = %s
              AND horario   = %s
              AND estado   != 'cancelada'
            """,
            (cita.doctor_id, cita.horario)
        )
        if cursor.fetchone():
            raise HTTPException(
                status_code=409,
                detail=f"El horario '{cita.horario}' ya está ocupado para el doctor {cita.doctor_id}."
            )

        # ── Insertar nueva cita ──
        nueva_id = str(uuid.uuid4())
        ahora    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute(
            """
            INSERT INTO citas (id, paciente_id, doctor_id, horario, estado, creado_en)
            VALUES (%s, %s, %s, %s, 'confirmada', %s)
            """,
            (nueva_id, cita.paciente_id, cita.doctor_id, cita.horario, ahora)
        )
        db.commit()

        nueva_cita = {
            "id":          nueva_id,
            "paciente_id": cita.paciente_id,
            "doctor_id":   cita.doctor_id,
            "horario":     cita.horario,
            "estado":      "confirmada",
            "creado_en":   ahora,
        }

        # ── Notificar coordinador (best-effort) ──
        await notificar_coordinador(nueva_cita)

        return CitaResponse(**nueva_cita)

    finally:
        # ── Cerrar recursos de BD ──
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()

        # ── Liberar lock solo si seguimos siendo los dueños ──
        current = r.get(lock_key)
        if current == lock_id:
            r.delete(lock_key)


@app.get("/citas")
def listar_citas(
    paciente_id: Optional[int] = Query(None, description="Filtrar por ID de paciente"),
    doctor_id:   Optional[int] = Query(None, description="Filtrar por ID de doctor"),
    incluir_canceladas: bool   = Query(False, description="Incluir citas canceladas"),
):
    """
    Lista citas activas. Opcionalmente filtra por paciente o doctor,
    y permite incluir las canceladas.
    """
    db     = get_db()
    cursor = db.cursor(dictionary=True)

    condiciones = []
    params      = []

    if not incluir_canceladas:
        condiciones.append("estado != 'cancelada'")

    if paciente_id is not None:
        condiciones.append("paciente_id = %s")
        params.append(paciente_id)

    if doctor_id is not None:
        condiciones.append("doctor_id = %s")
        params.append(doctor_id)

    query = "SELECT * FROM citas"
    if condiciones:
        query += " WHERE " + " AND ".join(condiciones)
    query += " ORDER BY creado_en DESC"

    cursor.execute(query, params)
    citas = cursor.fetchall()
    cursor.close()
    db.close()

    # Convertir datetimes a string para serialización JSON
    for c in citas:
        if isinstance(c.get("creado_en"), datetime):
            c["creado_en"] = c["creado_en"].strftime("%Y-%m-%d %H:%M:%S")

    return {"total": len(citas), "citas": citas}


@app.get("/citas/{cita_id}", response_model=CitaResponse)
def obtener_cita(cita_id: str):
    """Obtiene el detalle de una cita específica por su UUID."""
    db     = get_db()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM citas WHERE id = %s", (cita_id,))
    cita = cursor.fetchone()
    cursor.close()
    db.close()

    if not cita:
        raise HTTPException(status_code=404, detail="Cita no encontrada.")

    if isinstance(cita.get("creado_en"), datetime):
        cita["creado_en"] = cita["creado_en"].strftime("%Y-%m-%d %H:%M:%S")

    return cita


@app.delete("/cancelar_cita/{cita_id}")
def cancelar_cita(cita_id: str):
    """
    Cancela una cita existente (soft delete).
    Cambia el estado a 'cancelada' sin eliminar el registro.
    """
    db     = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM citas WHERE id = %s", (cita_id,))
        cita = cursor.fetchone()

        if not cita:
            raise HTTPException(status_code=404, detail="Cita no encontrada.")
        if cita["estado"] == "cancelada":
            raise HTTPException(status_code=400, detail="La cita ya está cancelada.")

        cursor.execute(
            "UPDATE citas SET estado = 'cancelada' WHERE id = %s",
            (cita_id,)
        )
        db.commit()
    finally:
        cursor.close()
        if db.is_connected():
            db.close()

    return {"mensaje": f"Cita {cita_id} cancelada exitosamente.", "cita_id": cita_id}
