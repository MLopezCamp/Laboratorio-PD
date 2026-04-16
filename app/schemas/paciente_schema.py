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