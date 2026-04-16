from app.db.database import SessionLocal
from app.repositories import paciente_repository
from app.schemas.paciente_schema import PacienteCreate
from fastapi import HTTPException

def crear_paciente(paciente: PacienteCreate):
    db = SessionLocal()

    try:
        if paciente_repository.get_by_email(db, paciente.email):
            raise HTTPException(400, "Ya existe un paciente con este correo")

        if paciente_repository.get_by_documento(db, paciente.documento):
            raise HTTPException(400, "Ya existe un paciente con este documento")

        return paciente_repository.create(db, paciente.dict())

    finally:
        db.close()


def listar_pacientes(page: int = 1, limit: int = 10, documento: str = None):
    db = SessionLocal()

    try:
        skip = (page - 1) * limit  # 🔥 AQUÍ está la corrección clave

        return paciente_repository.get_all_paginated(
            db,
            skip,
            limit,
            documento
        )

    finally:
        db.close()