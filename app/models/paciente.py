from sqlalchemy import Column, String
from app.db.database import Base

class Paciente(Base):
    __tablename__ = "pacientes"
    documento = Column(String, primary_key=True, unique=True)
    nombre = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)