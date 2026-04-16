from fastapi import APIRouter, Query
from app.schemas.paciente_schema import (
    PacienteCreate,
    PacienteResponse,
    PacientePaginatedResponse
)
from app.services.paciente_service import crear_paciente, listar_pacientes

router = APIRouter(prefix="/pacientes", tags=["Pacientes"])


@router.post("/crear_paciente", response_model=PacienteResponse)
def crear(paciente: PacienteCreate):
    return crear_paciente(paciente)


@router.get("/", response_model=PacientePaginatedResponse)
def listar(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    documento: str = None
):
    return listar_pacientes(page, limit, documento)