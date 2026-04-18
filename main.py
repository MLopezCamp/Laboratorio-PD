from fastapi import FastAPI, HTTPException
from database import r
import requests
import uvicorn

app = FastAPI(title="Servicio de Doctores - Grupo 3")


@app.get("/doctores")
def listar_doctores():
    """
    Obtiene la lista de todos los doctores registrados en Redis.
    """
    try:
        # Buscamos todas las llaves que empiecen con 'doc:'
        llaves = r.keys("doc:*")
        lista_doctores = []
        
        for llave in llaves:
            # Obtenemos los datos del Hash
            datos = r.hgetall(llave)
            
            # Extraemos el ID de la llave (ejemplo: de 'doc:1' sacamos '1')
            id_actual = llave.split(":")[1]
            
            # Añadimos la información a nuestra lista
            lista_doctores.append({
                "id": id_actual,
                "nombre": datos.get("nombre"),
                "cedula": datos.get("cedula"),
                "especialidad": datos.get("especialidad"),
                "estado": datos.get("estado")
            })
            
        return {"total": len(lista_doctores), "doctores": lista_doctores}
        
    except Exception:
        raise HTTPException(status_code=500, detail="Error al conectar con Redis para listar")

@app.post("/crear_doctor")
def crear_doctor(id_doctor: int, nombre: str, cedula: str, especialidad: str):
    """
    Registra un doctor con validaciones para evitar duplicados por:
    1. ID (Llave única)
    2. Cédula (Identificación oficial)
    3. Nombre
    """
    try:
        # 1. VALIDACIÓN POR ID (Directa en Redis)
        if r.exists(f"doc:{id_doctor}"):
            raise HTTPException(
                status_code=400, 
                detail=f"Error: El ID {id_doctor} ya está registrado."
            )

        # 2. VALIDACIÓN POR CÉDULA
        # Obtenemos todas las llaves de doctores para revisar sus datos internos
        llaves_existentes = r.keys("doc:*")
        
        for llave in llaves_existentes:
            datos_existentes = r.hgetall(llave)
            
            # Comparar Cédula
            if datos_existentes.get("cedula") == cedula:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Error: La cédula {cedula} ya pertenece a otro doctor."
                )
            
            

        # 3. Si pasa todas las validaciones, guardamos en Redis
        datos_nuevos = {
            "nombre": nombre,
            "cedula": cedula,
            "especialidad": especialidad,
            "estado": "activo"
        }
        
        r.hset(f"doc:{id_doctor}", mapping=datos_nuevos)
        
        return {
            "status": "success", 
            "mensaje": f"Doctor {nombre} registrado exitosamente."
        }
        
    except HTTPException as he:
        raise he
    except Exception:
        raise HTTPException(
            status_code=500, 
            detail="Error interno al validar o conectar con Redis local."
        )
    
@app.get("/disponibilidad")
def gestionar_disponibilidad(doctor_id: str, horario: str):
    # Paso 3: Manejo de Errores
    if not r.exists(f"doc:{doctor_id}"):
        raise HTTPException(status_code=404, detail="Doctor no encontrado")

    

    # Paso 3: Lock Distribuido
    lock_key = f"lock:{doctor_id}:{horario}"
    # Si r.set devuelve True, el bloqueo se realizó. Si es None, ya estaba ocupado.
    bloqueo_exitoso = r.set(lock_key, "OCUPADO", nx=True, ex=300)

    if not bloqueo_exitoso:
        # Paso 4: Validar errores de concurrencia
        raise HTTPException(
            status_code=409, 
            detail="Cita ya reservada por otro usuario (Lock Activo)"
        )

    return {
        "doctor_id": doctor_id,
        "horario": horario,
        "estado": "confirmado",
        "codigo_reserva": f"RES-{doctor_id}-{horario}"
    }

if __name__ == "__main__":
    # IMPORTANTE: host="0.0.0.0" para que sea visible en la red 172.16.0.x
    uvicorn.run(app, host="0.0.0.0", port=8003)
