from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from app.services.surtidores_service import SurtidoresService
from app.database import database

router = APIRouter()

class VentaManualRequest(BaseModel):
    consecutivo: str
    cara: int
    manguera: int
    producto_id: int
    fecha: str
    hora: str
    precio_galon: float
    volumen_galones: float
    valor_total: float
    promotor_id: int
    supervisor_id: int
    contingencia: bool = True

@router.get("/precios-mangueras")
async def obtener_precios_y_mangueras():
    """
    Obtiene el catálogo de Caras, Mangueras y Precios 
    para la interfaz de Contingencia.
    Equivalent to Java's SetupDao.getCaras()
    """
    try:
        resultado = await SurtidoresService.obtener_catalogo_mangueras_precios()
        if not resultado["exito"]:
            raise HTTPException(status_code=500, detail=resultado.get("error", "Error desconocido al obtener catálogo"))
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/registrar")
async def registrar_venta_manual(req: VentaManualRequest):
    """
    Registra una venta manual (contingencia) directamente en base de datos.
    Requiere el ID del supervisor autorizado.
    """
    try:
        # Aquí inyectamos el query análogo al que Java realiza para persistir la Venta de Contingencia.
        # Generalmente a la tabla 'transacciones' o 'facturacion_encabezado'.
        import uuid
        
        # Obtenemos el grado para la manguera seleccionada
        query_grado = "SELECT grado FROM surtidores_detalles WHERE cara = :cara AND manguera = :manguera LIMIT 1"
        row_grado = await database.fetch_one(query=query_grado, values={"cara": req.cara, "manguera": req.manguera})
        grado = row_grado["grado"] if row_grado else 0
        
        trama_json = f'{{"contingencia": true, "consecutivo": "{req.consecutivo}", "tipoVenta": "002"}}'
        
        # Asumiendo tabla transacciones (id idéntico a las autorizaciones del core)
        query_insert = """
            INSERT INTO transacciones (
                codigo, surtidor, cara, grado, preventa, estado,
                monto_maximo, cantidad_maxima, fecha_servidor, fecha_creacion,
                medio_autorizacion, trama, transaccion_sincronizada, promotor_id
            ) VALUES (
                :codigo, 1, :cara, :grado, true, 'A', :valor_total, :volumen,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'manual', CAST(:trama AS json), 'N', :promotor_id
            )
        """
        
        values = {
            "codigo": str(uuid.uuid4()),
            "cara": req.cara,
            "grado": grado,
            "valor_total": req.valor_total,
            "volumen": req.volumen_galones,
            "trama": trama_json,
            "promotor_id": req.promotor_id
        }
        
        await database.execute(query=query_insert, values=values)
        return {"exito": True, "mensaje": f"Factura de contingencia {req.consecutivo} registrada exitosamente"}
        
    except Exception as e:
        print(f"[VENTA_MANUAL] Error al registrar contingencia: {e}")
        raise HTTPException(status_code=500, detail=str(e))
