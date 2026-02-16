"""
Router para consultas de surtidores
"""
from fastapi import APIRouter, HTTPException

from app.database import database
from app.models.schemas import ListaEstadosSurtidores, EstadoSurtidor

router = APIRouter()

@router.get("/estados", response_model=ListaEstadosSurtidores)
async def obtener_estados_surtidores():
    """
    Obtener estados actuales de todos los surtidores
    """
    try:
        query = """
            SELECT 
                s.id,
                sd.cara,
                sd.manguera,
                COALESCE(sd.estado_publico, 1) as estado_codigo,
                CASE 
                    WHEN sd.estado_publico = 1 THEN 'ESPERA'
                    WHEN sd.estado_publico = 2 THEN 'DESCOLGADA'
                    WHEN sd.estado_publico = 3 THEN 'DESPACHO'
                    WHEN sd.estado_publico = 4 THEN 'FIN VENTA'
                    ELSE 'DESCONOCIDO'
                END as estado,
                COALESCE(p.descripcion, 'N/A') as producto,
                sd.precio_unitario
            FROM surtidores s
            INNER JOIN surtidores_detalles sd ON sd.surtidores_id = s.id
            LEFT JOIN productos p ON p.id = sd.productos_id
            ORDER BY sd.cara, sd.manguera
        """
        
        rows = await database.fetch_all(query)
        
        surtidores = []
        for row in rows:
            surtidores.append(EstadoSurtidor(
                id=row['id'],
                cara=row['cara'],
                manguera=row['manguera'],
                estado=row['estado'],
                estado_codigo=row['estado_codigo'],
                producto=row['producto'],
                precio_unitario=row['precio_unitario']
            ))
        
        return ListaEstadosSurtidores(
            total=len(surtidores),
            surtidores=surtidores
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando surtidores: {str(e)}")


@router.get("/configuracion")
async def obtener_configuracion_surtidores():
    """
    Obtener configuración de surtidores (islas, caras, mangueras)
    """
    try:
        query = """
            SELECT 
                s.id,
                s.surtidor,
                s.islas_id as isla,
                COUNT(sd.id) as total_mangueras
            FROM surtidores s
            LEFT JOIN surtidores_detalles sd ON sd.surtidores_id = s.id
            GROUP BY s.id, s.surtidor, s.islas_id
            ORDER BY s.islas_id, s.surtidor
        """
        
        rows = await database.fetch_all(query)
        
        return {
            "total": len(rows),
            "surtidores": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando configuración: {str(e)}")
