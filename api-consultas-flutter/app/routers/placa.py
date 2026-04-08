"""
Router PLACA.py
Maneja las lógicas de pre-autorización para GLP y Clientes Propios 
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import httpx
from datetime import datetime

from app.database import database
from app.config import settings

router = APIRouter()
LAZO_HOST = getattr(settings, 'LAZO_HOST', '127.0.0.1')

class MangueraPlacaInfo(BaseModel):
    surtidor: int
    cara: int
    manguera: int
    grado: int
    producto_id: int
    producto_descripcion: str
    producto_precio: float
    familia_id: int
    familia_descripcion: str
    bloqueado: bool = False
    motivo_bloqueo: Optional[str] = None

class PreAutorizarRequest(BaseModel):
    surtidor: int
    cara: int
    manguera: int
    grado: int
    placa: str
    odometro: str
    promotor_id: Optional[int] = None
    saldo: Optional[float] = None
    tipo_cupo: Optional[str] = None
    documento_cliente: Optional[str] = None
    nombre_cliente: Optional[str] = None
    medio_autorizacion: Optional[str] = None
    serial_dispositivo: Optional[str] = None
    producto_precio: Optional[float] = None

@router.get("/mangueras", response_model=List[MangueraPlacaInfo])
async def obtener_mangueras_placa(tipo: str = Query("normal", description="Tipo de consulta: 'normal' o 'glp'")):
    """
    Obtener mangueras disponibles para Pre-autorización (GLP o normal).
    """
    try:
        sql = """
            SELECT
                sur.surtidor,
                sur.cara,
                sur.manguera,
                sur.grado,
                sur.productos_id,
                p.descripcion AS producto_descripcion,
                p.precio AS producto_precio,
                pf.id AS familia_id,
                pf.codigo AS familia_descripcion,
                sur.bloqueo,
                sur.motivo_bloqueo
            FROM surtidores_detalles sur
            INNER JOIN productos p ON p.id = sur.productos_id
            INNER JOIN productos_familias pf ON p.familias = pf.id
            WHERE sur.estado_publico = 100
        """
        if tipo.lower() == 'glp':
            sql += " AND (pf.codigo = 'GLP' OR p.descripcion ILIKE '%GLP%')"
        else:
            sql += " AND (pf.codigo <> 'GLP' AND p.descripcion NOT ILIKE '%GLP%')"
            
        sql += " ORDER BY sur.cara, sur.manguera"
        
        rows = await database.fetch_all(sql)
        
        mangueras = []
        for row in rows:
            bloqueado = False
            if row["bloqueo"] is not None:
                bloqueado = str(row["bloqueo"]).upper() in ("S", "1", "TRUE")
            
            mangueras.append(MangueraPlacaInfo(
                surtidor=int(row["surtidor"]) if row["surtidor"] is not None else 0,
                cara=int(row["cara"]) if row["cara"] is not None else 0,
                manguera=int(row["manguera"]) if row["manguera"] is not None else 0,
                grado=int(row["grado"]) if row["grado"] is not None else 0,
                producto_id=int(row["productos_id"]) if row["productos_id"] is not None else 0,
                producto_descripcion=row["producto_descripcion"] or "N/A",
                producto_precio=float(row["producto_precio"] or 0),
                familia_id=int(row["familia_id"]) if row["familia_id"] is not None else 0,
                familia_descripcion=row["familia_descripcion"] or "N/A",
                bloqueado=bloqueado,
                motivo_bloqueo=row["motivo_bloqueo"],
            ))
            
        return mangueras
    except Exception as e:
        print(f"[PLACA] Error obteniendo mangueras: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo mangueras: {str(e)}")


@router.get("/caras-usadas")
async def verificar_cara_usada(cara: int = Query(..., description="Número de cara a consultar")):
    """
    Verifica si una cara ya se encuentra ocupada por una pre-autorización en ventas_curso.
    """
    try:
        query = "SELECT 1 FROM ventas_curso WHERE cara = :cara LIMIT 1"
        row = await database.fetch_one(query=query, values={"cara": cara})
        tiene_pre = True if row else False
        return {"cara": cara, "tiene_preautorizacion": tiene_pre}
    except Exception as e:
        print(f"[PLACA] Error verificando cara usada {cara}: {e}")
        return {"cara": cara, "tiene_preautorizacion": False, "error": str(e)}


@router.get("/validar-sicom/{placa}")
async def validar_placa_sicom(placa: str):
    """
    Validar vehículo en SICOM para carga de GLP.
    """
    try:
        url = f"http://{LAZO_HOST}:8010/v1.0/lazo-cliente/sicom/vehiculos/{placa.upper()}"
        print(f"[PLACA] Consultando SICOM en {url}")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            
        if resp.status_code == 200:
            data = resp.json()
            # el JSON puede venir envuelto en 'data' ({ "data": { ... } })
            real_data = data.get("data", data)
            return {
                "exito": True,
                "mensaje": "Vehículo validado en SICOM correctamente.",
                "marca": real_data.get("marca", ""),
                "capacidad": real_data.get("capacidadTanqueGlp", "") or real_data.get("capacidad", "")
            }
        else:
            msg = "Rechazado por SICOM."
            try:
                body = resp.json()
                msg = body.get("mensajeError", body.get("mensaje", msg))
            except:
                pass
            return {"exito": False, "mensaje": msg}
    except Exception as e:
        print(f"[PLACA] Error verificando SICOM para placa {placa}: {e}")
        return {"exito": False, "mensaje": f"No se pudo conectar a SICOM. Detalle: {str(e)}"}


@router.post("/pre-autorizar")
async def pre_autorizar_placa(req: PreAutorizarRequest):
    """
    Registrar intención de pre-autorización. 
    Flutter se encarga de guardar esto en StatusPump memory state,
    aquí simplemente generamos OK para confirmar el trigger.
    """
    try:
        print(f"[PLACA] Vehiculo Pre-Autorizado -> Cara {req.cara}, Placa: {req.placa}")
        return {
            "exito": True,
            "mensaje": f"Vehículo con placa {req.placa} ha sido pre-autorizado. Levante la manguera y presione inicio en el surtidor."
        }
    except Exception as e:
        print(f"[PLACA] Error en preAutorizarPlaca: {e}")
        return {"exito": False, "mensaje": f"Error registrando pre-autorización: {str(e)}"}
