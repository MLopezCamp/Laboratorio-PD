from pydantic import BaseModel, EmailStr
from typing import List

class PacienteCreate(BaseModel):
    nombre: str
    documento: str
    email: EmailStr


class PacienteResponse(BaseModel):
    documento: str
    nombre: str
    email: str

    class Config:
        from_attributes = True


class PacientePaginatedResponse(BaseModel):
    total: int
    skip: int
    limit: int
    data: List[PacienteResponse]