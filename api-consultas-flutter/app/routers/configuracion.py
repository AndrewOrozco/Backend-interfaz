"""
Router para consultas de configuración
"""
from fastapi import APIRouter, HTTPException

from app.database import database
from app.models.schemas import ConfiguracionEDS

router = APIRouter()

@router.get("/eds")
async def obtener_configuracion_eds():
    """
    Obtener configuración de la EDS (Estación de Servicio)
    """
    try:
        query = """
            SELECT 
                e.id,
                e.alias as nombre,
                e.nit,
                e.razon_social,
                e.direccion,
                e.telefono,
                e.correo,
                e.codigo
            FROM empresas e
            WHERE e.estado = 'A'
            LIMIT 1
        """
        
        row = await database.fetch_one(query)
        
        if not row:
            return {"mensaje": "No se encontró configuración de EDS"}
        
        return {
            "id": row['id'],
            "nombre": row['nombre'] or row['razon_social'],
            "razon_social": row['razon_social'],
            "nit": row['nit'],
            "direccion": row['direccion'],
            "telefono": row['telefono'],
            "correo": row['correo'],
            "codigo": row['codigo']
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando EDS: {str(e)}")


@router.get("/promotores")
async def obtener_promotores_activos():
    """
    Obtener promotores con turno activo
    """
    try:
        query = """
            SELECT 
                p.id,
                p.nombre,
                p.documento,
                t.fecha_apertura,
                t.id as turno_id
            FROM personas p
            INNER JOIN turnos t ON t.personas_id = p.id
            WHERE t.estado = 'A' 
              AND t.fecha_cierre IS NULL
            ORDER BY t.fecha_apertura DESC
        """
        
        rows = await database.fetch_all(query)
        
        return {
            "total": len(rows),
            "promotores": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando promotores: {str(e)}")


@router.get("/parametros")
async def obtener_parametros():
    """
    Obtener parámetros de configuración del sistema
    """
    try:
        query = """
            SELECT 
                parametro,
                valor,
                descripcion
            FROM parametros
            WHERE estado = 'A'
            ORDER BY parametro
        """
        
        rows = await database.fetch_all(query)
        
        return {
            "total": len(rows),
            "parametros": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando parámetros: {str(e)}")


@router.get("/medios-pago")
async def obtener_medios_pago():
    """
    Obtener medios de pago configurados
    """
    try:
        query = """
            SELECT 
                id,
                codigo,
                descripcion,
                CASE WHEN estado = 'A' THEN true ELSE false END as activo
            FROM medios_pagos
            ORDER BY descripcion
        """
        
        rows = await database.fetch_all(query)
        
        return {
            "total": len(rows),
            "medios_pago": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando medios de pago: {str(e)}")
