"""
Router para consultas de configuración
"""
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.database import database, database_registry
from app.services import license_service

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
                e.correo
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
            "alias": row['nombre'] or row['razon_social'],
            "razon_social": row['razon_social'],
            "nit": row['nit'],
            "direccion": row['direccion'],
            "telefono": row['telefono'],
            "correo": row['correo'],
            "codigo": ""
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando EDS: {str(e)}")


@router.get("/equipo/info")
async def obtener_equipo_info():
    """
    Obtener información del equipo/POS para Flutter
    """
    try:
        query = """
            SELECT 
                e.id,
                e.serial_equipo,
                e.ip,
                COALESCE((SELECT islas_id FROM surtidores LIMIT 1), 1) as numeroIsla
            FROM equipos e
            ORDER BY e.id
            LIMIT 1
        """
        
        row = await database.fetch_one(query)
        
        if not row:
            return {"mensaje": "No se encontró configuración de equipo"}
            
        return {
            "id": row['id'],
            "serial_equipo": row['serial_equipo'],
            "ip": row['ip'],
            "referencia": "",
            "numeroIsla": row['numeroisla']
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando equipo: {str(e)}")


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


# ============================================================
# TAG RFID
# ============================================================

@router.get("/usuarios-tag")
async def obtener_usuarios_tag():
    """
    Obtener lista de personas con su tag RFID.
    El campo tag vive directamente en la tabla personas.
    """
    try:
        query = """
            SELECT
                id,
                identificacion,
                nombre,
                estado,
                COALESCE(tag, '') AS tag
            FROM personas
            WHERE estado IN ('A', 'I')
            ORDER BY nombre
        """
        rows = await database.fetch_all(query)

        usuarios = [
            {
                "id": row["id"],
                "identificacion": str(row["identificacion"] or ""),
                "nombre": row["nombre"] or "",
                "estado": "ACTIVO" if str(row["estado"]).upper() in ("A", "ACTIVO") else "INACTIVO",
                "tag": row["tag"] or "",
            }
            for row in rows
        ]

        return {"total": len(usuarios), "usuarios": usuarios}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando usuarios tag: {str(e)}")


class RegistrarTagRequest(BaseModel):
    identificacion: str
    tag: str


@router.post("/registrar-tag")
async def registrar_tag(request: RegistrarTagRequest):
    """
    Asignar o actualizar el tag RFID de un usuario.
    Actualiza personas.tag directamente.
    """
    try:
        resultado = await database.execute(
            "UPDATE personas SET tag = :tag WHERE identificacion = :ident",
            {"tag": request.tag, "ident": request.identificacion}
        )

        if resultado == 0:
            return {"success": False, "message": f"Usuario {request.identificacion} no encontrado"}

        return {"success": True, "message": "Tag RFID asignado correctamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error registrando tag: {str(e)}")


@router.get("/lectura-tag")
async def obtener_lectura_tag():
    """
    Polling del lector RFID físico.
    Consulta la tabla lecturas_tag por la lectura más reciente no procesada.
    Flutter llama esto cada 3s para autocompletar el campo tag.
    """
    try:
        # Tomar la lectura más reciente sin procesar
        row = await database.fetch_one(
            """
            SELECT lectura
            FROM lecturas_tag
            ORDER BY fecha DESC
            LIMIT 1
            """
        )

        if row and row["lectura"]:
            return {"disponible": True, "lectura": str(row["lectura"])}

        return {"disponible": False, "lectura": None}

    except Exception as e:
        # Si la tabla no existe o falla, no bloqueamos el flujo
        return {"disponible": False, "lectura": None}


# ════════════════════════════════════════════════════════════
# LICENCIAS
# ════════════════════════════════════════════════════════════

class ActivarLicenciaRequest(BaseModel):
    code: str


@router.get("/licencia")
async def obtener_estado_licencia():
    """
    Retorna el estado de licencia actual del equipo.
    Incluye el fingerprint para que el administrador pueda generar el código.
    Este endpoint puede llamarse antes del login — no requiere auth.
    """
    return await license_service.get_license_status(database)


@router.post("/activar-licencia")
async def activar_licencia(body: ActivarLicenciaRequest, request: Request):
    """
    Valida y activa la licencia del equipo con el código proporcionado.
    ISO 27001: registra cada intento en licencias_audit.
    """
    if not body.code or len(body.code.strip()) < 10:
        raise HTTPException(status_code=400, detail="Código de licencia inválido (mínimo 10 caracteres)")

    ip_origen = request.client.host if request.client else "desconocido"
    return await license_service.activate_license(database, body.code.strip(), ip_origen)


@router.post("/restaurar-licencia")
async def restaurar_licencia(request: Request):
    """
    Restaura el equipo limpiando equipos y empresas en ambas BDs.
    Equivale al 'Formatear Equipo' de Java pero SIN borrar ventas/movimientos.
    ISO 27001: queda registrado en auditoría con IP y timestamp.
    """
    ip_origen = request.client.host if request.client else "desconocido"
    return await license_service.reset_license(database, database_registry, ip_origen)
