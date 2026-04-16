from fastapi import APIRouter
from typing import List

from app.schemas.paciente_schema import PacienteCreate, PacienteResponse
from app.services.paciente_service import crear_paciente, listar_pacientes

router = APIRouter(prefix="/pacientes", tags=["Pacientes"])


@router.post("/crear_paciente", response_model=PacienteResponse)
def crear(paciente: PacienteCreate):
    return crear_paciente(paciente)


@router.get("/", response_model=List[PacienteResponse])
def listar(documento: str = None):
    return listar_pacientes(documento)