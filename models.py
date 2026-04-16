from sqlalchemy import Column, Integer, String
from database import Base

class Notificacion(Base):
    __tablename__ = "notificaciones"

    id = Column(Integer, primary_key=True, index=True)
    paciente_id = Column(Integer)
    mensaje = Column(String)
    cita_id = Column(Integer)
