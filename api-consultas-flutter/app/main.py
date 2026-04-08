"""
API de Consultas para Flutter - FastAPI
=======================================
Backend para consultas del frontend Flutter.
Separado de LazoExpress para mantener responsabilidades claras.

Usa las mismas funciones SQL que la UI de Java:
    - fnc_consultar_ventas_pendientes(jornada_id, promotor_id, limite)
    - fnc_consultar_ventas(jornada_id, promotor_id, limite)
    - fnc_actualizar_medios_de_pagos(json)
    - fnc_consultar_medios_pago_imagenes(hayInternet, traerEfectivo)

Endpoints:
    /ventas/sin-resolver         - Ventas pendientes
    /ventas/historial            - Historial de ventas
    /ventas/resumen              - Resumen del día
    /ventas/tipos-identificacion - Tipos de identificación DIAN
    /ventas/consultar-cliente    - Consulta cliente externo (:7011)
    /ventas/medios-pago          - Medios de pago disponibles
    /ventas/medios-pago-venta    - Medios ya asignados a una venta
    /ventas/actualizar-medios-pago   - Guardar medios de pago (fnc_actualizar_medios_de_pagos)
    /ventas/actualizar-datos-venta   - Guardar datos de venta (placa, cliente)
    /surtidores/                 - Estado de surtidores
    /canastilla/                 - Productos canastilla
    /configuracion/              - Configuración EDS

Puerto: 8020
"""

import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.database import database, database_registry
from app.routers import ventas, surtidores, canastilla, configuracion, rumbo, turnos, fidelizacion, gopass, placa
from app.ws_notifications import router as ws_router
from backend_fe_7011.routers import fe_7011_router
from backend_fe_7011.fe_retry import worker_loop as fe_retry_worker_loop

# Lifespan para manejar conexión a DB
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: conectar a las bases de datos
    await database.connect()
    print("✅ Conectado a PostgreSQL (core)")
    await database_registry.connect()
    print("✅ Conectado a PostgreSQL (registry)")
    # Iniciar worker de reintentos FE en background
    retry_task = asyncio.create_task(fe_retry_worker_loop())
    print("✅ Worker reintentos FE iniciado")
    yield
    # Shutdown: cancelar worker y desconectar
    retry_task.cancel()
    try:
        await retry_task
    except asyncio.CancelledError:
        pass
    await database.disconnect()
    await database_registry.disconnect()
    print("❌ Desconectado de PostgreSQL")

# Crear app FastAPI
app = FastAPI(
    title="API Consultas Flutter",
    description="Backend para consultas del frontend Flutter POS - Ventas",
    version="1.0.0",
    lifespan=lifespan
)

# Configurar CORS para permitir conexiones desde Flutter de manera segura
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://127.0.0.1"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(ventas.router, prefix="/ventas", tags=["Ventas"])
app.include_router(surtidores.router, prefix="/surtidores", tags=["Surtidores"])
app.include_router(canastilla.router, prefix="/canastilla", tags=["Canastilla"])
app.include_router(configuracion.router, prefix="/configuracion", tags=["Configuración"])
app.include_router(rumbo.router, prefix="/rumbo", tags=["Rumbo"])
app.include_router(turnos.router, prefix="/turnos", tags=["Turnos"])
app.include_router(fidelizacion.router, prefix="/fidelizacion", tags=["Fidelización"])
app.include_router(gopass.router, prefix="/gopass", tags=["GoPass"])
app.include_router(placa.router, prefix="/placa", tags=["Placa"])
app.include_router(fe_7011_router)
app.include_router(ws_router)

@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "api-consultas-flutter",
        "version": "1.0.0"
    }

@app.get("/health", tags=["Health"])
async def health():
    """Health check con estado de la DB"""
    try:
        # Verificar conexión a DB
        await database.execute("SELECT 1")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "ok",
        "database": db_status
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8020)
