#!/usr/bin/env python3
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import time
import uuid
import uvicorn
from datetime import datetime

HOST = "0.0.0.0"
PORT = 8004

app = FastAPI(title="💳 Sistema de Pagos", version="1.0.0")

pagos_store = {}
pagos_lista = []

class PagoRequest(BaseModel):
    orden_id: str
    monto: float

@app.get("/")
def index():
    return {
        "servicio": "💳 Sistema de Pagos",
        "version": "1.0.0",
        "estado": "🟢 EN LÍNEA",
        "endpoints": {
            "POST /pagar": "Procesar un pago",
            "GET /pagos": "Listar pagos",
            "GET /estadisticas": "Ver estadísticas",
            "GET /docs": "Documentación"
        }
    }

@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/info")
def info():
    return {
        "acceso_local": f"http://localhost:{PORT}/",
        "acceso_red": "http://172.16.0.160:8004/",
        "documentacion": f"http://localhost:{PORT}/docs"
    }

@app.post("/pagar")
def procesar_pago(pago: PagoRequest):
    orden_id = pago.orden_id.strip()
    monto = pago.monto
    
    if not orden_id:
        raise HTTPException(status_code=400, detail="orden_id no puede estar vacío")
    
    if monto <= 0:
        raise HTTPException(status_code=400, detail="El monto debe ser mayor a 0")
    
    if f"pago:{orden_id}" in pagos_store:
        raise HTTPException(status_code=400, detail=f"La orden '{orden_id}' ya fue pagada")
    
    print(f"⏳ Procesando pago para orden '{orden_id}' por ${monto}...")
    time.sleep(2)
    
    pago_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    
    datos = {
        "pago_id": pago_id,
        "orden_id": orden_id,
        "monto": monto,
        "estado": "pagado",
        "timestamp": timestamp
    }
    
    pagos_store[f"pago:{orden_id}"] = datos
    pagos_lista.insert(0, datos)
    
    print(f"✅ Pago exitoso: {pago_id}")
    
    return {
        "mensaje": "✅ Pago procesado exitosamente",
        "pago_id": pago_id,
        "orden_id": orden_id,
        "monto": monto,
        "estado": "pagado",
        "timestamp": timestamp
    }

@app.get("/pagos")
def listar_pagos(limite: int = Query(100, ge=1, le=1000)):
    if not pagos_lista:
        return {"mensaje": "No hay pagos registrados", "total_pagos": 0, "pagos": []}
    
    return {
        "total_pagos": len(pagos_lista),
        "pagos_mostrados": len(pagos_lista[:limite]),
        "pagos": pagos_lista[:limite]
    }

@app.get("/estadisticas")
def obtener_estadisticas():
    if not pagos_lista:
        return {"total_transacciones": 0, "monto_total": 0.0, "monto_promedio": 0.0}
    
    montos = [p["monto"] for p in pagos_lista]
    return {
        "total_transacciones": len(pagos_lista),
        "monto_total": round(sum(montos), 2),
        "monto_promedio": round(sum(montos) / len(montos), 2),
        "monto_maximo": round(max(montos), 2),
        "monto_minimo": round(min(montos), 2)
    }

@app.delete("/limpiar")
def limpiar_datos():
    global pagos_store, pagos_lista
    cantidad = len(pagos_lista)
    pagos_store.clear()
    pagos_lista.clear()
    return {"mensaje": f"Se eliminaron {cantidad} pagos"}

@app.on_event("startup")
async def startup_event():
    print("\n" + "="*60)
    print("🚀 SERVIDOR DE PAGOS INICIADO")
    print("="*60)
    print(f"📍 Acceso local:     http://localhost:{PORT}/")
    print(f"📍 Acceso red:       http://172.16.0.160:{PORT}/")
    print(f"📚 Documentación:    http://localhost:{PORT}/docs")
    print("="*60 + "\n")

if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, reload=False, log_level="info")