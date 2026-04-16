from sqlalchemy.orm import Session
from app.models.paciente import Paciente

def get_by_email(db: Session, email: str):
    return db.query(Paciente).filter(Paciente.email == email).first()

def get_by_documento(db: Session, documento: str):
    return db.query(Paciente).filter(Paciente.documento == documento).first()

def create(db: Session, paciente_data):
    paciente = Paciente(**paciente_data)
    db.add(paciente)
    db.commit()
    db.refresh(paciente)
    return paciente


def get_all(db: Session, documento: str = None):
    query = db.query(Paciente)

    if documento:
        query = query.filter(Paciente.documento == documento)

    return query.all()