from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from app.database import database

router = APIRouter()

class AnulacionRequest(BaseModel):
    venta_id: int
    supervisor_id: int
    motivo_codigo: int
    promotor_id: int

@router.get("/consultar")
async def consultar_ventas_anulables(fecha_inicio: str = Query(...), fecha_fin: str = Query(...)):
    """
    Busca transacciones habilitadas para ser anuladas dentro de un rango de fechas.
    Equivalente al query de SECURE_CENTRAL_POINT_EMPLEADOS_VENTAS_ANULADAS_CONSULTA.
    """
    try:
        # Se asume formato yyyy-MM-dd
        query = """
            SELECT
                id as movimiento_id,
                codigo as prefijo,
                id as nro,
                fecha_creacion as fecha,
                promotor_id as promotor,
                monto_maximo as valor,
                estado
            FROM transacciones
            WHERE fecha_creacion >= :fecha_inicio 
              AND fecha_creacion <= :fecha_fin
              AND estado != 'C' -- Excluir las ya anuladas ('C'anceladas)
            ORDER BY fecha_creacion DESC
            LIMIT 100
        """
        # Asyncpg requires actual datetime objects for timestamp columns
        dt_inicio = datetime.strptime(f"{fecha_inicio} 00:00:00", "%Y-%m-%d %H:%M:%S")
        dt_fin = datetime.strptime(f"{fecha_fin} 23:59:59", "%Y-%m-%d %H:%M:%S")

        values = {
            "fecha_inicio": dt_inicio,
            "fecha_fin": dt_fin
        }
        
        filas = await database.fetch_all(query=query, values=values)
        
        ventas = []
        for row in filas:
            ventas.append({
                "movimiento_id": row["movimiento_id"],
                "prefijo": str(row["prefijo"])[:8], # Mostrar un fragmento del UUID como prefijo
                "nro": row["nro"] or row["movimiento_id"], # Fallback
                "fecha": str(row["fecha"]),
                "promotor": f"Promotor {row['promotor']}",
                "valor": float(row["valor"]) if row["valor"] else 0.0,
                "estado": row["estado"]
            })
            
        return {"exito": True, "data": ventas}
    except Exception as e:
        print(f"[ANULACIONES] Error al consultar: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/motivos")
async def obtener_motivos():
    """
    Retorna el catálogo de motivos válidos para anulación.
    Equivalente a ObtenerMotivosAnulacionUseCase en Java.
    """
    motivos = [
        {"codigo": 1, "descripcion": "ERROR DE DIGITACIÓN"},
        {"codigo": 2, "descripcion": "CLIENTE DESISTE DE COMPRA"},
        {"codigo": 3, "descripcion": "MANGUERA INCORRECTA"},
        {"codigo": 4, "descripcion": "CONTINGENCIA DEL SISTEMA"},
        {"codigo": 5, "descripcion": "OTRO"}
    ]
    return {"exito": True, "motivos": motivos}

@router.post("/ejecutar")
async def ejecutar_anulacion(req: AnulacionRequest):
    """
    Dada la autorización del supervisor, ejecuta la anulación de la venta.
    """
    try:
        # Modificar el estado a Anulado ('C'ancelado o similar según el Core)
        query_update = """
            UPDATE transacciones 
            SET estado = 'C', 
                observacion = :motivo
            WHERE id = :venta_id AND estado != 'C'
        """
        values = {
            "venta_id": req.venta_id,
            "motivo": f"Anulacion MotivoCod: {req.motivo_codigo} - Sup: {req.supervisor_id}"
        }
        
        rows_affected = await database.execute(query=query_update, values=values)
        
        if rows_affected > 0:
            return {"exito": True, "mensaje": f"Factura Nro {req.venta_id} anulada y revertida con éxito."}
        else:
            return {"exito": False, "mensaje": "La factura no existe o ya fue anulada."}
            
    except Exception as e:
        print(f"[ANULACIONES] Error al ejecutar: {e}")
        raise HTTPException(status_code=500, detail=str(e))

