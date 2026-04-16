from fastapi import FastAPI
from app.api.pacientes import router as pacientes_router

from app.db.database import Base, engine
from app.models.paciente import Paciente

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Servicio de Pacientes")

app.include_router(pacientes_router)

@app.get("/")
def root():
    return {"msg": "Servicio de pacientes activo"}