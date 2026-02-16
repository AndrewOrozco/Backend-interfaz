"""
Router para consultas de ventas
Usa las mismas funciones SQL que la UI de Java:
- fnc_consultar_ventas_pendientes(jornada_id, promotor_id, limite)
- fnc_consultar_ventas(jornada_id, promotor_id, limite)
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import json
import httpx

from app.database import database
from app.config import settings

router = APIRouter()

LAZO_HOST = getattr(settings, 'LAZO_HOST', 'localhost')

FIDELIZACION_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Encoding": "identity",
    "Authorization": "Basic cGFzc3BvcnR4OlQ0MUFYUWtYSjh6",
    "aplicacion": "lazoexpress",
    "dispositivo": "proyectos",
}


def parse_atributos(atributos_data) -> dict:
    """Parsear atributos JSON de forma segura
    
    Puede venir como:
    - dict (ya parseado por PostgreSQL)
    - str (JSON string)
    - None
    """
    try:
        if atributos_data is None:
            return {}
        if isinstance(atributos_data, dict):
            return atributos_data
        if isinstance(atributos_data, str) and atributos_data:
            return json.loads(atributos_data)
    except Exception as e:
        print(f"[API] Error parseando atributos: {e}")
    return {}


@router.get("/venta-activa-cara/{cara}")
async def obtener_venta_activa_por_cara(cara: int):
    """
    Obtener la venta activa (ct_movimientos) más reciente para una cara de surtidor.
    
    ct_movimientos NO tiene columna 'cara' directamente.
    La cara se obtiene a través de surtidores_detalles (que sí tiene cara y manguera).
    
    Estrategia de búsqueda (en orden):
    1. ct_movimientos JOIN surtidores_detalles (por surtidores_detalles_id o manguera)
    2. ventas_curso (tabla de ventas activas, tiene cara directamente)
    3. fnc_consultar_ventas_pendientes (función SQL que ya hace el join)
    """
    try:
        # Primero: descubrir cómo ct_movimientos se relaciona con la cara
        # Intentamos varios JOINs posibles
        
        # Intento 1: JOIN ct_movimientos con surtidores_detalles por surtidores_detalles_id
        movimiento = await _buscar_movimiento_por_cara(cara)
        
        if movimiento:
            print(f"[API] Venta activa para cara {cara}: movimiento_id={movimiento['movimiento_id']}, "
                  f"source={movimiento['source']}")
            return {
                "found": True,
                **movimiento,
                "cara": cara
            }
        
        return {
            "found": False,
            "message": f"No se encontró venta activa para cara {cara}",
            "movimiento_id": None,
            "cara": cara
        }
        
    except Exception as e:
        print(f"[API] Error buscando venta activa para cara {cara}: {e}")
        import traceback
        traceback.print_exc()
        return {
            "found": False,
            "message": str(e),
            "movimiento_id": None,
            "cara": cara
        }


async def _buscar_movimiento_por_cara(cara: int) -> dict | None:
    """
    Buscar el movimiento_id más reciente para una cara.
    
    IMPORTANTE: En ct_movimientos, la 'cara' NO es una columna directa.
    Está dentro del campo 'atributos' (JSONB).
    
    Java lo accede así (SqlQueryEnum.java):
        SELECT cm.id FROM ct_movimientos cm 
        WHERE cm.atributos::json->>'cara' = ?::text 
        ORDER BY cm.fecha DESC LIMIT 1
    
    Usamos CAST() en vez de :: para compatibilidad con SQLAlchemy.
    """
    
    # Estrategia 1: ct_movimientos con cara en atributos JSON
    # Java exacto: cm.atributos::json->>'cara' = ?::text
    try:
        query = """
            SELECT cm.id, cm.venta_total, cm.estado, cm.fecha
            FROM ct_movimientos cm 
            WHERE CAST(cm.atributos AS json)->>'cara' = :cara_text
              AND cm.fecha >= CURRENT_DATE
            ORDER BY cm.fecha DESC, cm.id DESC
            LIMIT 1
        """
        row = await database.fetch_one(query, {"cara_text": str(cara)})
        if row:
            d = dict(row)
            print(f"[API] Estrategia 1 OK: movimiento_id={d['id']}")
            return {
                "source": "ct_movimientos_atributos_json",
                "movimiento_id": d['id'],
                "monto": float(d.get('venta_total') or 0),
                "estado": d.get('estado'),
            }
        else:
            print(f"[API] Estrategia 1: No se encontró movimiento hoy para cara {cara}")
    except Exception as e:
        print(f"[API] Estrategia 1 falló (atributos json): {e}")
    
    # Estrategia 2: Buscar en TODAS las fechas (no solo hoy)
    try:
        query = """
            SELECT cm.id, cm.venta_total, cm.estado, cm.fecha
            FROM ct_movimientos cm 
            WHERE CAST(cm.atributos AS json)->>'cara' = :cara_text
            ORDER BY cm.id DESC
            LIMIT 1
        """
        row = await database.fetch_one(query, {"cara_text": str(cara)})
        if row:
            d = dict(row)
            print(f"[API] Estrategia 2 OK: movimiento_id={d['id']} (sin filtro fecha)")
            return {
                "source": "ct_movimientos_atributos_any_date",
                "movimiento_id": d['id'],
                "monto": float(d.get('venta_total') or 0),
                "estado": d.get('estado'),
            }
    except Exception as e:
        print(f"[API] Estrategia 2 falló: {e}")
    
    # Estrategia 3: fnc_consultar_ventas_pendientes y filtrar por cara
    try:
        jornada_query = """
            SELECT grupo_jornada as jornada_id, personas_id as promotor_id 
            FROM jornadas WHERE fecha_fin IS NULL 
            ORDER BY fecha_inicio DESC LIMIT 1
        """
        jornada_row = await database.fetch_one(jornada_query)
        jornada_id = jornada_row['jornada_id'] if jornada_row else 0
        promotor_id = jornada_row['promotor_id'] if jornada_row else 0
        
        query = "SELECT * FROM fnc_consultar_ventas_pendientes(:jornada_id, :promotor_id, :limite)"
        rows = await database.fetch_all(query, {
            "jornada_id": jornada_id,
            "promotor_id": promotor_id,
            "limite": 1000,
        })
        
        for row in rows:
            d = dict(row)
            if d.get('cara') == cara:
                print(f"[API] Estrategia 3 OK: movimiento_id={d.get('numero')}")
                return {
                    "source": "fnc_ventas_pendientes",
                    "movimiento_id": d.get('numero'),
                    "monto": float(d.get('total') or 0),
                }
    except Exception as e:
        print(f"[API] Estrategia 3 falló (fnc_consultar_ventas_pendientes): {e}")
    
    return None


@router.get("/jornada-activa")
async def obtener_jornada_activa():
    """
    Obtener la jornada activa actual
    
    Retorna el jornada_id y personas_id (promotor) de la jornada abierta
    """
    try:
        query = """
            SELECT 
                j.grupo_jornada as jornada_id,
                j.personas_id as promotor_id,
                j.fecha_inicio,
                COALESCE(p.nombre, 'Sin nombre') as promotor_nombre
            FROM jornadas j
            LEFT JOIN personas p ON p.id = j.personas_id
            WHERE j.fecha_fin IS NULL
            ORDER BY j.fecha_inicio DESC
            LIMIT 1
        """
        
        row = await database.fetch_one(query)
        
        if row:
            return {
                "jornada_id": row['jornada_id'],  # grupo_jornada
                "promotor_id": row['promotor_id'],  # personas_id
                "fecha_inicio": str(row['fecha_inicio']),
                "promotor_nombre": row['promotor_nombre']
            }
        else:
            return {
                "jornada_id": 0,
                "promotor_id": 0,
                "fecha_inicio": None,
                "promotor_nombre": None,
                "mensaje": "No hay jornada activa"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo jornada: {str(e)}")


@router.get("/sin-resolver")
async def obtener_ventas_sin_resolver(
    jornada_id: int = Query(None, description="ID de jornada (auto si no se envía)"),
    promotor_id: int = Query(0, description="ID de promotor (0 = todos)"),
    limite: int = Query(50, ge=1, le=100, description="Límite de resultados"),
    pagina: int = Query(1, ge=1, description="Número de página")
):
    """
    Obtener ventas sin resolver (pendientes)
    
    Usa la función SQL: fnc_consultar_ventas_pendientes(jornada_id, promotor_id, limite)
    
    Si no se envía jornada_id, obtiene automáticamente la jornada activa.
    
    Incluye:
    - Ventas pendientes de Facturación Electrónica
    - Ventas pendientes de Datafono
    - Ventas pendientes de asignar cliente
    """
    try:
        # Si no se envía jornada_id, obtener la jornada activa (fecha_fin IS NULL)
        # Usamos grupo_jornada, NO el id de la tabla
        if jornada_id is None:
            jornada_query = """
                SELECT grupo_jornada as jornada_id, personas_id as promotor_id 
                FROM jornadas 
                WHERE fecha_fin IS NULL 
                ORDER BY fecha_inicio DESC 
                LIMIT 1
            """
            jornada_row = await database.fetch_one(jornada_query)
            if jornada_row:
                jornada_id = jornada_row['jornada_id']  # grupo_jornada
                if promotor_id == 0:
                    promotor_id = jornada_row['promotor_id']  # personas_id
            else:
                jornada_id = 0
            print(f"[API] Jornada activa: {jornada_id}, Promotor: {promotor_id}")
        
        # Usar la misma función que Java - pasar límite alto para obtener todos
        # y luego paginar en Python
        query = "SELECT * FROM fnc_consultar_ventas_pendientes(:jornada_id, :promotor_id, :limite_sql)"
        
        rows = await database.fetch_all(
            query, 
            {"jornada_id": jornada_id, "promotor_id": promotor_id, "limite_sql": 1000}
        )
        
        # Paginación manual
        total_registros = len(rows)
        inicio = (pagina - 1) * limite
        fin = inicio + limite
        rows_paginadas = list(rows[inicio:fin]) if inicio < len(rows) else []
        
        ventas = []
        for row in rows_paginadas:
            row_dict = dict(row)
            atributos = parse_atributos(row_dict.get('atributos', '{}'))
            
            # Determinar el proceso/estado
            proceso = row_dict.get('proceso', '')
            if not proceso:
                estado_datafono = row_dict.get('descripcion_transaccion_estado_datafono', '')
                pendiente_cliente = row_dict.get('ind_pendiente_asignar_cliente', False)
                pendiente_adblue = row_dict.get('ind_pendiente_resolver_adblue', False)
                
                if estado_datafono == 'PENDIENTE' or pendiente_cliente:
                    proceso = 'FE'
                elif pendiente_adblue:
                    proceso = 'UREA'
                elif row_dict.get('codigo_autorizacion_datafono'):
                    proceso = 'Datafono'
                else:
                    proceso = 'Pendiente'
            
            # Obtener consecutivo/prefijo desde atributos.consecutivo
            # Fallback: usar columna 'consecutivo' de la función SQL
            prefijo = "N/A"
            if 'consecutivo' in atributos:
                cons_obj = atributos['consecutivo']
                if isinstance(cons_obj, dict):
                    pref = str(cons_obj.get('prefijo', ''))
                    cons = str(cons_obj.get('consecutivo_actual', ''))
                    if pref and cons:
                        prefijo = f"{pref}-{cons}"
                    elif pref:
                        prefijo = pref
            
            # Fallback: si no hay consecutivo en atributos, usar columna de la función
            if prefijo == "N/A":
                consecutivo_col = row_dict.get('consecutivo')
                if consecutivo_col:
                    prefijo = str(consecutivo_col)
            
            ventas.append({
                "id": row_dict.get('numero'),
                "prefijo": prefijo,
                "fecha": str(row_dict.get('fecha', '')),
                "producto": row_dict.get('producto', 'N/A'),
                "cara": row_dict.get('cara'),
                "cantidad": row_dict.get('cantidad', 0),
                "unidad": row_dict.get('unidad_medida', 'GL'),
                "total": row_dict.get('total', 0),
                "operador": row_dict.get('operador', 'N/A'),
                "proceso": proceso,
                "estado_datafono": row_dict.get('descripcion_transaccion_estado_datafono', ''),
                "placa": atributos.get('vehiculo_placa', ''),
                "codigo_autorizacion": row_dict.get('codigo_autorizacion_datafono')
            })
        
        # Calcular páginas
        total_paginas = (total_registros + limite - 1) // limite if total_registros > 0 else 1
        
        return {
            "total": total_registros,
            "pagina": pagina,
            "por_pagina": limite,
            "total_paginas": total_paginas,
            "jornada_id": jornada_id,
            "ventas": ventas
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando ventas: {str(e)}")


@router.get("/historial")
async def obtener_historial_ventas(
    jornada_id: int = Query(None, description="ID de jornada (auto si no se envía)"),
    promotor_id: int = Query(0, description="ID de promotor (0 = todos)"),
    limite: int = Query(50, ge=1, le=100, description="Límite de resultados"),
    pagina: int = Query(1, ge=1, description="Número de página")
):
    """
    Obtener historial de ventas
    
    Usa la función SQL: fnc_consultar_ventas(jornada_id, promotor_id, limite)
    
    Si no se envía jornada_id, obtiene automáticamente la jornada activa.
    """
    try:
        # Si no se envía jornada_id, obtener la jornada activa (fecha_fin IS NULL)
        # Usamos grupo_jornada, NO el id de la tabla
        if jornada_id is None:
            jornada_query = """
                SELECT grupo_jornada as jornada_id, personas_id as promotor_id 
                FROM jornadas 
                WHERE fecha_fin IS NULL 
                ORDER BY fecha_inicio DESC 
                LIMIT 1
            """
            jornada_row = await database.fetch_one(jornada_query)
            if jornada_row:
                jornada_id = jornada_row['jornada_id']  # grupo_jornada
                if promotor_id == 0:
                    promotor_id = jornada_row['promotor_id']  # personas_id
            else:
                jornada_id = 0
            print(f"[API] Historial - Jornada: {jornada_id}, Promotor: {promotor_id}")
        
        # Usar la misma función que Java - pasar límite alto para obtener todos
        # y luego paginar en Python
        query = "SELECT * FROM fnc_consultar_ventas(:jornada_id, :promotor_id, :limite_sql)"
        
        rows = await database.fetch_all(
            query, 
            {"jornada_id": jornada_id, "promotor_id": promotor_id, "limite_sql": 1000}
        )
        
        # Paginación manual
        total_registros = len(rows)
        inicio = (pagina - 1) * limite
        fin = inicio + limite
        rows_paginadas = list(rows[inicio:fin]) if inicio < len(rows) else []
        
        ventas = []
        for row in rows_paginadas:
            row_dict = dict(row)
            atributos = parse_atributos(row_dict.get('atributos', '{}'))
            
            # Obtener consecutivo/prefijo desde atributos.consecutivo
            # Fallback: usar columna 'consecutivo' de la función SQL
            prefijo = "N/A"
            if 'consecutivo' in atributos:
                cons_obj = atributos['consecutivo']
                if isinstance(cons_obj, dict):
                    pref = str(cons_obj.get('prefijo', ''))
                    cons = str(cons_obj.get('consecutivo_actual', ''))
                    if pref and cons:
                        prefijo = f"{pref}-{cons}"
                    elif pref:
                        prefijo = pref
            
            # Fallback: si no hay consecutivo en atributos, usar columna de la función
            if prefijo == "N/A":
                consecutivo_col = row_dict.get('consecutivo')
                if consecutivo_col:
                    prefijo = str(consecutivo_col)
            
            # Flag de fidelización (Java: ct_movimientos.atributos.fidelizada = "S")
            fue_fidelizada = atributos.get('fidelizada', 'N') == 'S'

            ventas.append({
                "id": row_dict.get('numero'),
                "prefijo": prefijo,
                "fecha": str(row_dict.get('fecha', '')),
                "producto": row_dict.get('producto', 'N/A'),
                "cara": row_dict.get('cara'),
                "cantidad": row_dict.get('cantidad', 0),
                "unidad": row_dict.get('unidad_medida', 'GL'),
                "total": row_dict.get('total', 0),
                "operador": row_dict.get('operador', 'N/A'),
                "placa": atributos.get('vehiculo_placa', ''),
                "id_transmision": row_dict.get('id_transmision'),
                "fidelizada": fue_fidelizada,
            })
        
        # Calcular páginas
        total_paginas = (total_registros + limite - 1) // limite if total_registros > 0 else 1
        
        return {
            "total": total_registros,
            "pagina": pagina,
            "por_pagina": limite,
            "total_paginas": total_paginas,
            "jornada_id": jornada_id,
            "ventas": ventas
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando historial: {str(e)}")


@router.get("/resumen")
async def obtener_resumen_ventas():
    """
    Obtener resumen de ventas del día
    """
    try:
        query = """
            SELECT 
                COUNT(*) as total_ventas,
                COALESCE(SUM(monto), 0) as monto_total,
                COALESCE(SUM(volumen), 0) as volumen_total,
                COUNT(CASE WHEN tipo = '001' THEN 1 END) as ventas_combustible,
                COUNT(CASE WHEN tipo = '005' THEN 1 END) as ventas_canastilla,
                COUNT(CASE WHEN sincronizado::text = '0' THEN 1 END) as ventas_pendientes
            FROM ct_movimientos
            WHERE fecha >= CURRENT_DATE
              AND estado = 'A'
        """
        
        row = await database.fetch_one(query)
        
        return {
            "fecha": datetime.now().strftime('%Y-%m-%d'),
            "total_ventas": row['total_ventas'] or 0,
            "monto_total": float(row['monto_total'] or 0),
            "volumen_total": float(row['volumen_total'] or 0),
            "ventas_combustible": row['ventas_combustible'] or 0,
            "ventas_canastilla": row['ventas_canastilla'] or 0,
            "ventas_pendientes": row['ventas_pendientes'] or 0
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo resumen: {str(e)}")


# ============================================================
# CLIENTES Y TIPOS DE IDENTIFICACIÓN
# ============================================================

@router.get("/tipos-identificacion")
async def obtener_tipos_identificacion():
    """
    Obtener los tipos de identificación disponibles
    
    Consulta la tabla facturacion_electronica.identificacion_dian
    """
    try:
        query = """
            SELECT 
                tipo_de_identificacion, 
                codigo_identificacion, 
                aplica_fidelizacion, 
                caracteres_permitidos, 
                limite_caracteres 
            FROM facturacion_electronica.identificacion_dian 
            ORDER BY CASE 
                WHEN tipo_de_identificacion = 'Cedula de ciudadania' THEN 0 
                ELSE 1 
            END, tipo_de_identificacion
        """
        
        rows = await database.fetch_all(query)
        
        tipos = []
        for row in rows:
            tipos.append({
                "nombre": row['tipo_de_identificacion'],
                "codigo": row['codigo_identificacion'],
                "aplica_fidelizacion": row['aplica_fidelizacion'],
                "caracteres_permitidos": row['caracteres_permitidos'],
                "limite_caracteres": row['limite_caracteres']
            })
        
        # Si no hay tipos en la BD, devolver los predeterminados
        if not tipos:
            tipos = [
                {"nombre": "Cedula de ciudadania", "codigo": 13, "aplica_fidelizacion": True, "caracteres_permitidos": "0123456789", "limite_caracteres": 10},
                {"nombre": "NIT", "codigo": 31, "aplica_fidelizacion": True, "caracteres_permitidos": "0123456789", "limite_caracteres": 15},
                {"nombre": "Cedula de extranjeria", "codigo": 22, "aplica_fidelizacion": True, "caracteres_permitidos": "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", "limite_caracteres": 15},
                {"nombre": "Consumidor final", "codigo": 42, "aplica_fidelizacion": False, "caracteres_permitidos": "0123456789", "limite_caracteres": 12},
            ]
        
        return {"tipos": tipos}
        
    except Exception as e:
        print(f"[API] Error obteniendo tipos identificación: {e}")
        # Si la tabla no existe, devolver tipos predeterminados
        return {
            "tipos": [
                {"nombre": "Cedula de ciudadania", "codigo": 13, "aplica_fidelizacion": True, "caracteres_permitidos": "0123456789", "limite_caracteres": 10},
                {"nombre": "NIT", "codigo": 31, "aplica_fidelizacion": True, "caracteres_permitidos": "0123456789", "limite_caracteres": 15},
                {"nombre": "Cedula de extranjeria", "codigo": 22, "aplica_fidelizacion": True, "caracteres_permitidos": "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", "limite_caracteres": 15},
                {"nombre": "Consumidor final", "codigo": 42, "aplica_fidelizacion": False, "caracteres_permitidos": "0123456789", "limite_caracteres": 12},
            ]
        }


@router.get("/consultar-cliente")
async def consultar_cliente(
    identificacion: str = Query(..., description="Número de identificación del cliente"),
    tipo_documento: int = Query(13, description="Código del tipo de documento (13=CC, 31=NIT, etc)")
):
    """
    Consultar cliente por número de identificación
    
    Llama al servicio externo de Terpel:
    https://[host]:7011/proxi.terpel/consultarCliente
    
    El host se obtiene de whacer_parametros.direccion y se cachea.
    """
    from app.url_global import consultar_cliente_externo
    
    try:
        # Consultar al servicio externo
        resultado = await consultar_cliente_externo(
            database=database,
            documento_cliente=identificacion,
            tipo_documento=tipo_documento
        )
        
        if resultado["success"] and resultado["data"]:
            data = resultado["data"]
            
            # Extraer datos del cliente de la respuesta del servicio Terpel
            # Estructura: nombreRazonSocial, correoElectronico, telefono, direccion, etc.
            nombre = (
                data.get("nombreRazonSocial") or 
                data.get("nombreComercial") or 
                "CONSUMIDOR FINAL"
            ).strip()
            
            # Si el nombre está vacío, usar CONSUMIDOR FINAL
            if not nombre:
                nombre = "CONSUMIDOR FINAL"
            
            return {
                "encontrado": nombre != "CONSUMIDOR FINAL",
                "cliente": {
                    "id": None,
                    "identificacion": data.get("numeroDocumento", identificacion),
                    "nombre": nombre,
                    "email": data.get("correoElectronico"),
                    "telefono": data.get("telefono"),
                    "direccion": data.get("direccion"),
                    "ciudad": data.get("ciudad"),
                    "departamento": data.get("departamento"),
                    "tipo_identificacion": data.get("tipoDocumento", tipo_documento),
                    "regimen_fiscal": data.get("regimenFiscal"),
                    "tipo_responsabilidad": data.get("tipoResponsabilidad"),
                    "codigo_sap": data.get("codigoSAP"),
                    "raw_response": data  # Respuesta completa por si se necesita
                }
            }
        else:
            # Servicio no disponible o cliente no encontrado
            # Devolver datos para consumidor final
            return {
                "encontrado": False,
                "cliente": {
                    "id": None,
                    "identificacion": identificacion,
                    "nombre": "CONSUMIDOR FINAL",
                    "email": None,
                    "telefono": None,
                    "direccion": None,
                    "tipo_identificacion": "CONSUMIDOR FINAL"
                }
            }
        
    except Exception as e:
        # Si hay error, devolver cliente genérico
        return {
            "encontrado": False,
            "cliente": {
                "id": None,
                "identificacion": identificacion,
                "nombre": "CONSUMIDOR FINAL",
                "email": None,
                "telefono": None,
                "direccion": None,
                "tipo_identificacion": "CONSUMIDOR FINAL"
            },
            "error": str(e)
        }


@router.get("/medios-pago-venta/{movimiento_id}")
async def obtener_medios_pago_venta(movimiento_id: int):
    """
    Obtener los medios de pago ya asignados a una venta específica
    
    Consulta: ct_movimientos_medios_pagos + ct_medios_pagos
    """
    try:
        query = """
            SELECT 
                cmmp.id,
                cmmp.ct_medios_pagos_id,
                cmmp.ct_movimientos_id,
                cmmp.valor_recibido,
                cmmp.valor_cambio,
                cmmp.valor_total,
                cmmp.numero_comprobante,
                cmmp.codigo_dian,
                cmp.descripcion,
                cmp.id AS id_medio_pago
            FROM ct_movimientos_medios_pagos cmmp 
            INNER JOIN ct_medios_pagos cmp ON cmmp.ct_medios_pagos_id = cmp.id 
            WHERE cmmp.ct_movimientos_id = :movimiento_id
        """
        
        rows = await database.fetch_all(query, {"movimiento_id": movimiento_id})
        
        medios = []
        for row in rows:
            row_dict = dict(row)
            medios.append({
                "id": row_dict.get('id'),
                "medio_pago_id": row_dict.get('ct_medios_pagos_id'),
                "nombre": row_dict.get('descripcion', 'SIN NOMBRE'),
                "voucher": row_dict.get('numero_comprobante', ''),
                "valor": float(row_dict.get('valor_total', 0)),
                "valor_recibido": float(row_dict.get('valor_recibido', 0)),
                "valor_cambio": float(row_dict.get('valor_cambio', 0)),
                "codigo_dian": row_dict.get('codigo_dian'),
            })
        
        print(f"[API] Medios de pago para venta {movimiento_id}: {len(medios)}")
        for m in medios:
            print(f"  - {m['nombre']}: ${m['valor']}")
        
        return {"medios": medios, "movimiento_id": movimiento_id}
        
    except Exception as e:
        print(f"[API] Error obteniendo medios de venta: {e}")
        return {"medios": [], "error": str(e)}


@router.get("/medios-pago")
async def obtener_medios_pago(
    hay_internet: bool = Query(True, description="Si hay conexión a internet"),
    traer_efectivo: bool = Query(True, description="Si incluir efectivo en la lista")
):
    """
    Obtener los medios de pago disponibles
    
    Usa la función SQL: fnc_consultar_medios_pago_imagenes(hayInternet, traerEfectivo)
    
    Java solo filtra id == 20000 (bono Vive Terpel).
    NO filtra EFECTIVO, GOPASS, ni MI EMPRESA.
    
    La función retorna columnas: id, descripcion, atributos, codigo_dian,
    id_medio_pago_recurso, id_medio_pago_recurso_seleccionado
    """
    try:
        # Usar la función SQL para obtener medios de pago
        query = """
            SELECT * FROM public.fnc_consultar_medios_pago_imagenes(:hay_internet, :traer_efectivo)
        """
        
        rows = await database.fetch_all(query, {
            "hay_internet": hay_internet,
            "traer_efectivo": traer_efectivo
        })
        
        medios = []
        for row in rows:
            row_dict = dict(row)
            
            # Debug: log todas las columnas de la primera fila
            if not medios:
                print(f"[API] Columnas de fnc_consultar_medios_pago_imagenes: {list(row_dict.keys())}")
            
            # Java: medio.setId(re.getLong("id"))
            medio_id = row_dict.get('id')
            
            # Java: medio.setDescripcion(re.getString("descripcion"))
            nombre = row_dict.get('descripcion') or 'SIN NOMBRE'
            
            # Java solo filtra id == 20000 (bono Vive Terpel)
            # RecoverMedioPagoImage: if(data.getId()!= 20000L) listMedio.add(data)
            if medio_id == 20000:
                print(f"[API] Medio filtrado (bono 20000): {nombre}")
                continue
            
            # Determinar si requiere voucher basado en atributos o nombre
            nombre_upper = nombre.upper()
            requiere_voucher = (
                'TARJETA' in nombre_upper or
                'APP' in nombre_upper or
                'BONO' in nombre_upper or
                'SODEXO' in nombre_upper or
                'RAPPI' in nombre_upper or
                'EDENRED' in nombre_upper or
                'BIGPASS' in nombre_upper or
                'EXITO' in nombre_upper or
                'FALABELLA' in nombre_upper or
                'TRANSFERENCIA' in nombre_upper or
                'CREDITO CLIENTES' in nombre_upper or
                'DATAFONO' in nombre_upper or
                'CON DATAFONO' in nombre_upper
            )
            
            medios.append({
                "id": medio_id,
                "codigo": row_dict.get('codigo'),
                "nombre": nombre,
                "codigo_dian": row_dict.get('codigo_dian'),
                "requiere_voucher": requiere_voucher,
                "imagen": row_dict.get('imagen'),
            })
        
        # Log TODOS los medios con su ID para debug
        print(f"[API] Medios de pago encontrados: {len(medios)}")
        for m in medios:
            print(f"  - id={m['id']} nombre='{m['nombre']}' codigo_dian={m['codigo_dian']}")
        
        # Si no hay medios, devolver predeterminados
        if not medios:
            medios = [
                {"id": 1, "codigo": "01", "nombre": "EFECTIVO", "codigo_dian": 10, "requiere_voucher": False},
            ]
        
        return {"medios": medios, "hay_internet": hay_internet}
        
    except Exception as e:
        print(f"[API] Error obteniendo medios de pago: {e}")
        import traceback
        traceback.print_exc()
        return {
            "medios": [
                {"id": 1, "codigo": "01", "nombre": "EFECTIVO", "codigo_dian": 10, "requiere_voucher": False},
            ],
            "error": str(e)
        }


# ============================================================
# ACTUALIZAR MEDIOS DE PAGO
# ============================================================

from pydantic import BaseModel
from typing import List, Optional

class MedioPagoInput(BaseModel):
    ct_medios_pagos_id: int
    descripcion: Optional[str] = ""  # Nombre del medio (EFECTIVO, APP TERPEL, etc.)
    valor_total: float
    valor_recibido: float
    valor_cambio: float = 0
    codigo_dian: Optional[int] = None
    numero_comprobante: Optional[str] = ""

class ActualizarMediosPagoRequest(BaseModel):
    movimiento_id: int
    medios_pagos: List[MedioPagoInput]
    identificador_equipo: Optional[str] = None

class ActualizarDatosVentaRequest(BaseModel):
    movimiento_id: int
    placa: Optional[str] = None
    odometro: Optional[int] = None
    nombre_cliente: Optional[str] = None
    identificacion_cliente: Optional[str] = None
    tipo_documento: Optional[int] = None  # Codigo del tipo de documento (13=CC, 31=NIT)
    orden: Optional[str] = None
    es_credito: Optional[bool] = False


@router.post("/actualizar-medios-pago")
async def actualizar_medios_pago(request: ActualizarMediosPagoRequest):
    """
    Actualizar los medios de pago de una venta
    
    Llama a: SELECT * FROM fnc_actualizar_medios_de_pagos(?::json)
    
    IMPORTANTE: El JSON debe usar los mismos nombres de campos que Java:
    - "identificadorMovimiento" (NO "ct_movimientos_id")
    - "identificadorEquipo" (NO "equipo_id")
    - "validarTurno": false
    - "mediosDePagos" (NO "medios_pagos")
    
    Cada medio debe tener:
    - "ct_medios_pagos_id", "descripcion", "valor_recibido", "valor_cambio",
      "valor_total", "voucher", "moneda_local", "numero_comprobante",
      "ing_pago_datafono", "codigo_dian"
    """
    try:
        import json
        
        # Construir el JSON con EXACTAMENTE la misma estructura que Java
        # Ver: MedioPagosConfirmarViewController.sendMedioPago()
        medios_array = []
        for m in request.medios_pagos:
            medios_array.append({
                "ct_medios_pagos_id": m.ct_medios_pagos_id,
                "descripcion": m.descripcion or "",
                "valor_recibido": m.valor_recibido,
                "valor_cambio": m.valor_cambio,
                "valor_total": m.valor_total,
                "voucher": m.numero_comprobante or "",
                "moneda_local": "",
                "numero_comprobante": m.numero_comprobante or "",
                "ing_pago_datafono": False,
                "id_adquiriente": "",
                "franquicia": "",
                "tipo_cuenta": "",
                "id_transaccion": "",
                "numero_recibo": "",
                "id": 0,
                "trm": 0,
                "bin": "",
                "codigo_dian": m.codigo_dian or 0,
                "confirmacionBono": False
            })
        
        medios_json = {
            "identificadorMovimiento": request.movimiento_id,
            "identificadorEquipo": int(request.identificador_equipo) if request.identificador_equipo else 1,
            "validarTurno": False,
            "mediosDePagos": medios_array
        }
        
        json_str = json.dumps(medios_json)
        
        print(f"[API] Actualizando medios de pago para movimiento {request.movimiento_id}")
        print(f"[API] JSON: {json_str[:500]}...")
        
        # Llamar a la función SQL (misma que Java)
        # Escapar comillas simples en el JSON para SQL
        json_str_escaped = json_str.replace("'", "''")
        # Alias "completado" igual que Java:
        # Java: "select * from fnc_actualizar_medios_de_pagos(?::json) completado;"
        query = f"SELECT * FROM fnc_actualizar_medios_de_pagos('{json_str_escaped}'::json) completado"
        result = await database.fetch_one(query)
        
        completado = False
        if result:
            result_dict = dict(result)
            # La función retorna una columna "completado" (boolean)
            completado = result_dict.get('completado', result_dict.get('fnc_actualizar_medios_de_pagos', False))
        
        print(f"[API] Resultado fnc_actualizar_medios_de_pagos: completado={completado}, result_dict={result_dict if result else 'None'}")
        
        if not completado:
            print(f"[API] ⚠ fnc_actualizar_medios_de_pagos retornó False para movimiento {request.movimiento_id}")
            print(f"[API] ⚠ JSON enviado: {json_str}")
        
        return {
            "success": completado,
            "message": "Venta gestionada correctamente" if completado else "Error al actualizar medios de pago en la función SQL",
            "movimiento_id": request.movimiento_id,
            "completado": completado
        }
        
    except Exception as e:
        print(f"[API] Error actualizando medios de pago: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": str(e),
            "movimiento_id": request.movimiento_id
        }


# ============================================================
# GUARDAR MEDIO DE PAGO EN VENTAS_CURSO (Status Pump)
# ============================================================
# Java desde Status Pump NO llama a fnc_actualizar_medios_de_pagos.
# En vez de eso, actualiza ventas_curso.atributos con:
# - DatosFactura: {medio_pago: ID, placa: "ABC123", numero_comprobante: "", odometro: ""}
# - gopass_v2: {placa: "ABC123"} (solo si es GOPASS)
# - isAppTerpel: true (solo si es APP TERPEL)
# - statusPump: true
# Cuando la venta termina, estos datos se copian a ct_movimientos.atributos
# y el medio de pago ya está asignado en "ventas sin resolver".

class GuardarMedioVentaCursoRequest(BaseModel):
    cara: int
    medio_pago_id: int               # ID del medio de pago (ej: 20004 = GOPASS, 20005 = APP TERPEL)
    medio_pago_descripcion: str = ""  # Nombre del medio (ej: "GOPASS", "APP TERPEL", "EFECTIVO")
    placa: Optional[str] = ""
    numero_comprobante: Optional[str] = ""
    odometro: Optional[str] = ""
    es_gopass: bool = False
    es_app_terpel: bool = False


@router.post("/guardar-medio-ventas-curso")
async def guardar_medio_ventas_curso(request: GuardarMedioVentaCursoRequest):
    """
    Guardar medio de pago en ventas_curso (flujo Status Pump).
    
    Replica EXACTAMENTE el flujo de Java:
    1. sdao.eliminarAtributosVenta(cara) - limpia gopass_v2, datafono, isAppTerpel
    2. sdao.updateVentasEncurso(recibo, datosMedio, EFECTIVO) - guarda DatosFactura
    3. Si es GOPASS: sdao.updateVentasEncurso(recibo, placa, GOPASS) - guarda gopass_v2
    
    Esto hace que cuando la venta termina y aparece en "ventas sin resolver",
    el medio de pago ya esté asignado y no se tenga que seleccionar de nuevo.
    """
    import json
    
    try:
        print(f"[API] Guardando medio en ventas_curso - cara={request.cara}, "
              f"medio={request.medio_pago_descripcion}, placa={request.placa}")
        
        # 1. Obtener atributos actuales de ventas_curso
        query = "SELECT atributos FROM ventas_curso WHERE cara = :cara"
        row = await database.fetch_one(query, {"cara": request.cara})
        
        if not row:
            return {
                "success": False,
                "message": f"No se encontró venta en curso para cara {request.cara}"
            }
        
        atributos = {}
        if row['atributos']:
            if isinstance(row['atributos'], str):
                atributos = json.loads(row['atributos'])
            else:
                atributos = dict(row['atributos'])
        
        # 2. Limpiar atributos previos (Java: eliminarAtributosVenta)
        # Elimina gopass_v2, datafono, isAppTerpel
        for key in ['gopass_v2', 'datafono', 'isAppTerpel']:
            atributos.pop(key, None)
        
        # 3. Construir DatosFactura (Java: buildJsonMedioPagoEfectivo)
        datos_factura = atributos.get('DatosFactura', {})
        if not isinstance(datos_factura, dict):
            datos_factura = {}
        
        datos_factura['medio_pago'] = request.medio_pago_id
        datos_factura['numero_comprobante'] = request.numero_comprobante or ""
        datos_factura['placa'] = request.placa or ""
        datos_factura['odometro'] = request.odometro or ""
        atributos['DatosFactura'] = datos_factura
        
        # 4. Si es GOPASS, agregar gopass_v2 con la placa
        if request.es_gopass and request.placa:
            atributos['gopass_v2'] = {"placa": request.placa}
            # Java: aplicarImprimirFalse para factura_electronica y remision
            for key in ['factura_electronica', 'remision']:
                if key in atributos and isinstance(atributos[key], dict):
                    atributos[key]['pendiente_impresion'] = False
                    atributos[key]['imprimir'] = False
        
        # 4b. Si es APP TERPEL, agregar isAppTerpel = true
        # Java: atributos.addProperty("isAppTerpel", true)
        if request.es_app_terpel:
            atributos['isAppTerpel'] = True
            # Java: aplicarImprimirFalse para factura_electronica y remision
            for key in ['factura_electronica', 'remision']:
                if key in atributos and isinstance(atributos[key], dict):
                    atributos[key]['pendiente_impresion'] = False
                    atributos[key]['imprimir'] = False
        
        # 5. statusPump flag
        atributos['statusPump'] = 'factura_electronica' in atributos
        
        # 6. UPDATE ventas_curso (Java: "update ventas_curso set atributos=?::json where cara=?")
        update_query = """
            UPDATE ventas_curso 
            SET atributos = CAST(:atributos AS json) 
            WHERE cara = :cara
        """
        await database.execute(update_query, {
            "atributos": json.dumps(atributos),
            "cara": request.cara
        })
        
        print(f"[API] ventas_curso actualizado OK para cara {request.cara}")
        print(f"[API] Atributos: {json.dumps(atributos)[:300]}...")
        
        return {
            "success": True,
            "message": f"Medio de pago guardado en venta curso cara {request.cara}",
            "cara": request.cara,
            "medio": request.medio_pago_descripcion,
            "placa": request.placa if request.es_gopass else None
        }
        
    except Exception as e:
        print(f"[API] Error guardando medio en ventas_curso: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": str(e),
            "cara": request.cara
        }


# ============================================================
# GUARDAR DATOS FACTURA EN VENTAS_CURSO (desde Gestionar Venta)
# ============================================================
# Java flow (SurtidorDao.generarDatosSurtidorVentasCurso):
# 1. Lee atributos de ventas_curso WHERE cara = ?
# 2. Guarda DatosFactura: { medio_pago, placa, odometro, numero_comprobante }
# 3. Guarda factura_electronica: { ...respuesta Terpel completa con extraData... }
# 4. statusPump = true (si factura_electronica existe)
# 5. UPDATE ventas_curso SET atributos = ?::json WHERE cara = ?
#
# Este endpoint replica ese flujo COMPLETO para que el Java LazoExpress
# reciba los datos de FE correctos y la venta NO vaya a "sin resolver".
# ============================================================

class GuardarDatosFacturaVentasCursoRequest(BaseModel):
    cara: int
    # Datos del cliente (factura_electronica)
    factura_electronica: Optional[dict] = None  # Objeto completo del cliente Terpel
    tipo_documento: Optional[int] = 13
    identificacion_cliente: Optional[str] = "222222222222"
    nombre_cliente: Optional[str] = "CONSUMIDOR FINAL"
    # Datos vehiculo
    placa: Optional[str] = ""
    odometro: Optional[str] = ""
    # Opciones
    fidelizar: bool = False
    facturacion_electronica: bool = False


@router.post("/guardar-datos-factura-ventas-curso")
async def guardar_datos_factura_ventas_curso(request: GuardarDatosFacturaVentasCursoRequest):
    """
    Guardar datos de factura (cliente + FE) en ventas_curso.
    
    Replica el flujo de Java: SurtidorDao.generarDatosSurtidorVentasCurso()
    - Tipo PLACA: guarda DatosFactura
    - Tipo FACTURA_ELECTRONICA: guarda factura_electronica (datos Terpel completos)
    - statusPump = true si factura_electronica existe
    
    IMPORTANTE: Esto va a ventas_curso (venta ACTIVA en la bomba),
    NO a ct_movimientos. Cuando la venta termina, Java copia estos
    atributos a ct_movimientos automáticamente.
    """
    import json as json_lib
    
    try:
        print(f"[API] Guardando datos factura en ventas_curso - cara={request.cara}")
        print(f"[API] Cliente: {request.nombre_cliente} ({request.identificacion_cliente})")
        print(f"[API] FE: {request.facturacion_electronica}, Fidelizar: {request.fidelizar}")
        
        # 1. Obtener atributos actuales de ventas_curso
        query = "SELECT atributos FROM ventas_curso WHERE cara = :cara"
        row = await database.fetch_one(query, {"cara": request.cara})
        
        if not row:
            return {
                "success": False,
                "message": f"No se encontró venta en curso para cara {request.cara}"
            }
        
        atributos = {}
        if row['atributos']:
            if isinstance(row['atributos'], str):
                atributos = json_lib.loads(row['atributos'])
            else:
                atributos = dict(row['atributos'])
        
        # 2. Actualizar/mantener DatosFactura
        datos_factura = atributos.get('DatosFactura', {})
        if not isinstance(datos_factura, dict):
            datos_factura = {}
        
        # Mantener medio_pago existente si ya fue asignado, sino EFECTIVO (1)
        if 'medio_pago' not in datos_factura:
            datos_factura['medio_pago'] = 1
        
        datos_factura['placa'] = (request.placa or "").upper()
        datos_factura['odometro'] = request.odometro or ""
        if 'numero_comprobante' not in datos_factura:
            datos_factura['numero_comprobante'] = ""
        
        atributos['DatosFactura'] = datos_factura
        
        # 3. Construir factura_electronica (el objeto COMPLETO que Java espera)
        # Java: SurtidorDao.updateVentasEncurso(recibo, datos, FACTURA_ELECTRONICA)
        if request.factura_electronica and isinstance(request.factura_electronica, dict):
            # Usar la respuesta completa de Terpel como base
            fe_obj = dict(request.factura_electronica)
            
            # Agregar campos adicionales que Java agrega
            fe_obj['identificacion_cliente'] = request.tipo_documento or 13
            fe_obj['documentoCliente'] = request.identificacion_cliente or "222222222222"
            
            # Asegurar campo descripcionTipoDocumento
            if 'descripcionTipoDocumento' not in fe_obj:
                tipos_desc = {
                    13: "Cédula de ciudadanía",
                    31: "NIT",
                    22: "Cédula de extranjería",
                    42: "Documento de identificación extranjero",
                }
                fe_obj['descripcionTipoDocumento'] = tipos_desc.get(
                    request.tipo_documento, "Cédula de ciudadanía"
                )
            
            fe_obj['pendiente_impresion'] = True
            fe_obj['sinSistema'] = fe_obj.get('sinSistema', False)
            fe_obj['errorFaltaCampos'] = fe_obj.get('errorFaltaCampos', False)
            
            atributos['factura_electronica'] = fe_obj
        elif request.facturacion_electronica:
            # FE habilitada pero sin datos Terpel → construir objeto mínimo
            atributos['factura_electronica'] = {
                "identificacion_cliente": request.tipo_documento or 13,
                "documentoCliente": request.identificacion_cliente or "222222222222",
                "numeroDocumento": request.identificacion_cliente or "222222222222",
                "nombreComercial": request.nombre_cliente or "CONSUMIDOR FINAL",
                "nombreRazonSocial": request.nombre_cliente or "CONSUMIDOR FINAL",
                "pendiente_impresion": True,
                "sinSistema": False,
                "errorFaltaCampos": False,
                "asignarCliente": True,
            }
        
        # 4. Datos de cliente en atributos (igual que Java)
        atributos['personas_nombre'] = request.nombre_cliente or "CONSUMIDOR FINAL"
        atributos['personas_identificacion'] = request.identificacion_cliente or "222222222222"
        
        # 5. Fidelización automática (replica Java: SurtidorDao.updateVentasEncurso con FIDELIZACION=1)
        # Java guarda DatosFidelizacion en ventas_curso.atributos para que
        # LazoExpress 8010 lo lea al registrar la venta y acumule puntos automáticamente.
        if request.fidelizar and request.identificacion_cliente:
            try:
                estacion_row = await database.fetch_one(
                    "SELECT codigo_empresa FROM empresas LIMIT 1"
                )
                codigo_estacion = estacion_row["codigo_empresa"] if estacion_row else "EDS1"

                # Mapear tipo_documento DIAN a código fidelización
                # Java: RenderizarProcesosFidelizacionyFE.tiposIdentificaionFidelizaicon
                tipo_fid_map = {13: "CC", 22: "CE", 41: "PAS"}
                tipo_fid = tipo_fid_map.get(request.tipo_documento or 13, "CC")

                fecha_tx = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # Puerto 10462 espera ConsultClientRequestBody (no estructura plana)
                validation_body = {
                    "transactionData": {
                        "origenVenta": "EDS",
                        "identificacionPuntoVenta": codigo_estacion,
                        "fechaTransaccion": fecha_tx,
                    },
                    "customer": {
                        "codigoTipoIdentificacion": tipo_fid,
                        "numeroIdentificacion": request.identificacion_cliente,
                    },
                    "idIntegracion": None,
                }

                # Java usa puerto 10462 (proxy local lazo.loyalty), NO 8010
                # LoyaltiesEndpoints.CONSULTARCLIENTE = "http://127.0.0.1:10462/lazo.loyalty/customer/validate/"
                url_validar = "http://127.0.0.1:10462/lazo.loyalty/customer/validate/"
                print(f"[API] Fidelización automática: validando {request.identificacion_cliente}")
                print(f"[API] GET {url_validar}")

                async with httpx.AsyncClient(timeout=30.0) as client:
                    fid_resp = await client.request(
                        "GET",
                        url_validar,
                        json=validation_body,
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                        },
                    )

                if fid_resp.status_code == 200:
                    fid_data = fid_resp.json()
                    print(f"[API] Fidelización response: {json_lib.dumps(fid_data, default=str)[:500]}")
                    # Puerto 10462 devuelve FoundClient: {nombreCliente, existeClient, ...}
                    nombre_fid = fid_data.get("nombreCliente", "")
                    existe_client = fid_data.get("existeClient", False)

                    if existe_client and nombre_fid and nombre_fid not in ("NO REGISTRADO", "CLIENTE", ""):
                        # Usar la respuesta completa del 10462 como DatosFidelizacion
                        # Ya tiene la estructura FoundClient: {nombreCliente, existeClient, datosCliente, ...}
                        # Asegurar que datosCliente.customer tenga la cedula sin encriptar
                        datos_fidelizacion = dict(fid_data)
                        if "datosCliente" in datos_fidelizacion and "customer" in datos_fidelizacion["datosCliente"]:
                            datos_fidelizacion["datosCliente"]["customer"]["numeroIdentificacion"] = request.identificacion_cliente
                        datos_fidelizacion["fidelizarMarket"] = False
                        atributos['DatosFidelizacion'] = datos_fidelizacion
                        atributos['fidelizada'] = "S"
                        print(f"[API] DatosFidelizacion guardado OK: {nombre_fid}")
                    else:
                        print(f"[API] Cliente no apto fidelización: existeClient={existe_client} nombre={nombre_fid}")
                        atributos['fidelizada'] = "N"
                else:
                    print(f"[API] Fidelización validation HTTP {fid_resp.status_code}")
                    atributos['fidelizada'] = "N"
            except Exception as fid_err:
                print(f"[API] Error validando fidelización (continuando): {fid_err}")
                atributos['fidelizada'] = "N"
        else:
            atributos['fidelizada'] = "N"
        
        # 6. statusPump flag (Java: statusPump = factura_electronica exists)
        atributos['statusPump'] = 'factura_electronica' in atributos
        
        # 7. UPDATE ventas_curso
        update_query = """
            UPDATE ventas_curso 
            SET atributos = CAST(:atributos AS json) 
            WHERE cara = :cara
        """
        atributos_json = json_lib.dumps(atributos)
        await database.execute(update_query, {
            "atributos": atributos_json,
            "cara": request.cara
        })
        
        print(f"[API] ventas_curso actualizado OK para cara {request.cara}")
        print(f"[API] statusPump={atributos.get('statusPump')}")
        print(f"[API] factura_electronica={'SI' if 'factura_electronica' in atributos else 'NO'}")
        print(f"[API] Atributos: {atributos_json[:500]}...")
        
        return {
            "success": True,
            "message": "Datos de factura guardados correctamente",
            "cara": request.cara,
            "statusPump": atributos.get('statusPump', False),
        }
        
    except Exception as e:
        print(f"[API] Error guardando datos factura en ventas_curso: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": str(e),
            "cara": request.cara
        }


# ============================================================
# LIMPIAR FLAG isAppTerpel DE VENTAS_CURSO
# ============================================================
# Después de que una venta con APP TERPEL termina de despachar,
# se debe limpiar el flag para que la siguiente venta en la misma
# cara NO herede el medio de pago APP TERPEL.
# ============================================================

class LimpiarAppTerpelRequest(BaseModel):
    cara: int

@router.post("/limpiar-appterpel-ventas-curso")
async def limpiar_appterpel_ventas_curso(request: LimpiarAppTerpelRequest):
    """
    Limpiar flag isAppTerpel de ventas_curso para una cara.
    Se llama después de que la venta con APP TERPEL termina y se envía al orquestador,
    para evitar que la siguiente venta en la misma cara herede el flag.
    """
    import json
    
    try:
        print(f"[API] Limpiando isAppTerpel de ventas_curso para cara {request.cara}")
        
        # Obtener atributos actuales
        query = "SELECT atributos FROM ventas_curso WHERE cara = :cara"
        row = await database.fetch_one(query, {"cara": request.cara})
        
        if not row or not row['atributos']:
            return {"success": True, "message": "No hay atributos que limpiar"}
        
        atributos = {}
        if isinstance(row['atributos'], str):
            atributos = json.loads(row['atributos'])
        else:
            atributos = dict(row['atributos'])
        
        # Verificar si tiene isAppTerpel
        tenia_flag = atributos.pop('isAppTerpel', None)
        
        if tenia_flag is None:
            return {"success": True, "message": "No tenía isAppTerpel asignado"}
        
        # Limpiar también DatosFactura.medio_pago si apuntaba a APP TERPEL
        # (resetear a efectivo = 1)
        datos_factura = atributos.get('DatosFactura', {})
        if isinstance(datos_factura, dict):
            datos_factura['medio_pago'] = 1  # Reset a EFECTIVO
            datos_factura['numero_comprobante'] = ""
            atributos['DatosFactura'] = datos_factura
        
        # Actualizar ventas_curso
        update_query = """
            UPDATE ventas_curso 
            SET atributos = CAST(:atributos AS json) 
            WHERE cara = :cara
        """
        await database.execute(update_query, {
            "atributos": json.dumps(atributos),
            "cara": request.cara
        })
        
        print(f"[API] isAppTerpel limpiado de ventas_curso cara {request.cara}")
        
        return {
            "success": True,
            "message": f"isAppTerpel limpiado para cara {request.cara}"
        }
        
    except Exception as e:
        print(f"[API] Error limpiando isAppTerpel: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "message": str(e)}


# ============================================================
# GOPASS - CONSULTA DE PLACAS
# ============================================================

class ConsultarPlacasGopassRequest(BaseModel):
    cara: int
    isla: Optional[str] = None      # Si no se envía, se obtiene de la BD
    surtidor: Optional[str] = None  # Si no se envía, se obtiene de la BD


@router.post("/gopass/consultar-placas")
async def consultar_placas_gopass(request: ConsultarPlacasGopassRequest):
    """
    Consultar placas GOPASS disponibles para una cara de surtidor.
    
    Hace proxy al servicio CentralPoint GOPASS en puerto 7011.
    
    Java equivalente: GopassFacade.consultarPlacasGopass()
    URL destino: http://localhost:7011/api/placaEnCurso
    Request: {"isla": "1", "cara": 1, "surtidor": "1"}
    Response: {"datos": [{"placa": "ABC123", "tagGopass": "...", "nombreUsuario": "...", ...}]}
    """
    import httpx
    
    try:
        isla = request.isla
        surtidor = request.surtidor
        
        # Si no se envía isla/surtidor, obtenerlos de la BD
        if not isla or not surtidor:
            query = """
                SELECT s.surtidor, s.islas_id as isla
                FROM surtidores s
                INNER JOIN surtidores_detalles sd ON sd.surtidores_id = s.id
                WHERE sd.cara = :cara
                LIMIT 1
            """
            row = await database.fetch_one(query, {"cara": request.cara})
            
            if not row:
                return {
                    "success": False,
                    "message": f"No se encontró surtidor para cara {request.cara}",
                    "placas": []
                }
            
            isla = isla or str(row['isla'])
            surtidor = surtidor or str(row['surtidor'])
        
        # Llamar al servicio GOPASS CentralPoint
        # Java: GopassFacade -> http://localhost:7011/api/placaEnCurso
        url = "http://localhost:7011/api/placaEnCurso"
        body = {
            "isla": isla,
            "cara": request.cara,
            "surtidor": surtidor
        }
        
        print(f"[GOPASS] Consultando placas: url={url}, body={body}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Verificar errores en la respuesta
                if "error" in data and data["error"]:
                    error_obj = data["error"]
                    if isinstance(error_obj, dict):
                        error_msg = error_obj.get("mensajeError", "Error desconocido en GOPASS")
                    else:
                        error_msg = str(error_obj)
                    print(f"[GOPASS] Error en respuesta: {error_msg}")
                    return {"success": False, "message": error_msg, "placas": []}
                
                # Extraer placas
                placas_raw = data.get("datos", [])
                placas = []
                for p in placas_raw:
                    placas.append({
                        "placa": p.get("placa", ""),
                        "tag_gopass": p.get("tagGopass", ""),
                        "nombre_usuario": p.get("nombreUsuario", ""),
                        "isla": p.get("isla", ""),
                        "fechahora": p.get("fechahora", "")
                    })
                
                print(f"[GOPASS] Placas encontradas: {len(placas)}")
                for pl in placas:
                    print(f"  - {pl['placa']} ({pl['nombre_usuario']})")
                
                return {
                    "success": True,
                    "placas": placas,
                    "message": f"Se encontraron {len(placas)} placas"
                }
            else:
                print(f"[GOPASS] Error HTTP {response.status_code}: {response.text}")
                return {
                    "success": False,
                    "message": f"Error del servicio GOPASS: HTTP {response.status_code}",
                    "placas": []
                }
    
    except httpx.TimeoutException:
        print("[GOPASS] Timeout consultando placas")
        return {
            "success": False,
            "message": "Timeout consultando placas GOPASS - intente nuevamente",
            "placas": []
        }
    except httpx.ConnectError:
        print("[GOPASS] No se pudo conectar al servicio en puerto 7011")
        return {
            "success": False,
            "message": "No se pudo conectar al servicio GOPASS (puerto 7011) - verifique que esté activo",
            "placas": []
        }
    except Exception as e:
        print(f"[GOPASS] Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error consultando placas: {str(e)}",
            "placas": []
        }


@router.post("/actualizar-datos-venta")
async def actualizar_datos_venta(request: ActualizarDatosVentaRequest):
    """
    Actualizar los datos de una venta (placa, odómetro, orden, cliente)
    
    Replica el comportamiento EXACTO de Java:
    1. Lee atributos JSON actual de ct_movimientos
    2. Agrega/actualiza datos dentro del JSON:
       - vehiculo_placa, vehiculo_odometro, vehiculo_numero (vehiculo)
       - personas_nombre, personas_identificacion (cliente)
       - cliente (objeto completo con extraData para FE)
    3. UPDATE ct_movimientos SET atributos = ?::json, estado = 'M' WHERE id = ?
    4. CALL prc_registrar_cliente_movimiento(movimiento_id, 0, 2, '{}'::json)
    
    NO existen columnas personas_nombre ni personas_identificacion en ct_movimientos.
    Todo va dentro del campo atributos (JSONB).
    """
    try:
        print(f"[API] Actualizando datos de venta {request.movimiento_id}")
        print(f"[API] Datos: placa={request.placa}, odometro={request.odometro}, "
              f"orden={request.orden}, cliente={request.nombre_cliente}, "
              f"identificacion={request.identificacion_cliente}")
        
        import json
        
        # 1. Obtener atributos actuales de ct_movimientos
        get_atributos = "SELECT atributos FROM ct_movimientos WHERE id = :movimiento_id"
        row = await database.fetch_one(get_atributos, {"movimiento_id": request.movimiento_id})
        
        if not row:
            return {
                "success": False,
                "message": f"No se encontró el movimiento {request.movimiento_id}",
                "movimiento_id": request.movimiento_id
            }
        
        atributos = {}
        if row['atributos']:
            if isinstance(row['atributos'], str):
                atributos = json.loads(row['atributos'])
            else:
                atributos = dict(row['atributos'])
        
        # 2. Actualizar datos de vehiculo en atributos JSON
        #    Java: atributosVenta.addProperty("vehiculo_placa", placa)
        if request.placa:
            atributos['vehiculo_placa'] = request.placa.upper()
        if request.odometro:
            atributos['vehiculo_odometro'] = request.odometro
        if request.orden:
            atributos['vehiculo_numero'] = request.orden
        
        # 3. Actualizar datos de cliente dentro de atributos JSON
        #    Java: atributos.addProperty("personas_nombre", nombre)
        #    Java: atributos.addProperty("personas_identificacion", identificacion)
        if request.nombre_cliente:
            atributos['personas_nombre'] = request.nombre_cliente
        if request.identificacion_cliente:
            atributos['personas_identificacion'] = request.identificacion_cliente
        
        # 4. Construir objeto cliente completo (para FE / transmision)
        #    Java: atributos.add("cliente", clienteJson)
        if request.nombre_cliente or request.identificacion_cliente:
            cliente_obj = atributos.get('cliente', {})
            if not isinstance(cliente_obj, dict):
                cliente_obj = {}
            
            cliente_obj['numeroDocumento'] = request.identificacion_cliente or cliente_obj.get('numeroDocumento', '222222222222')
            cliente_obj['nombreRazonSocial'] = request.nombre_cliente or cliente_obj.get('nombreRazonSocial', 'CONSUMIDOR FINAL')
            cliente_obj['nombreComercial'] = request.nombre_cliente or cliente_obj.get('nombreComercial', 'CONSUMIDOR FINAL')
            if request.tipo_documento:
                cliente_obj['tipoDocumento'] = request.tipo_documento
            
            atributos['cliente'] = cliente_obj
        
        if request.es_credito:
            atributos['es_credito'] = True
        
        # 5. UPDATE ct_movimientos: solo atributos y estado
        #    Java exacto: "update ct_movimientos set atributos=?::json, estado = 'M' where id=?"
        #    Usar CAST() para evitar conflicto con SQLAlchemy named params
        update_query = """
            UPDATE ct_movimientos 
            SET atributos = CAST(:atributos AS json),
                estado = 'M'
            WHERE id = :movimiento_id
        """
        
        atributos_json = json.dumps(atributos)
        print(f"[API] UPDATE ct_movimientos SET atributos=...json, estado='M' WHERE id={request.movimiento_id}")
        
        await database.execute(update_query, {
            "movimiento_id": request.movimiento_id,
            "atributos": atributos_json,
        })
        
        # 6. Llamar al procedimiento prc_registrar_cliente_movimiento
        #    Java: "call prc_registrar_cliente_movimiento(?, 0, 2, '{}'::json)"
        try:
            prc_query = f"CALL prc_registrar_cliente_movimiento({request.movimiento_id}, 0, 2, '{{}}' ::json)"
            await database.execute(prc_query)
            print(f"[API] prc_registrar_cliente_movimiento ejecutado OK")
        except Exception as prc_err:
            # El procedimiento puede no existir en todas las instalaciones
            print(f"[API] prc_registrar_cliente_movimiento no disponible: {prc_err}")
        
        return {
            "success": True,
            "message": "Datos de venta actualizados correctamente",
            "movimiento_id": request.movimiento_id
        }
        
    except Exception as e:
        print(f"[API] Error actualizando datos de venta: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": str(e),
            "movimiento_id": request.movimiento_id
        }


# ============================================================
# VALIDAR BOTONES APP TERPEL
# ============================================================
# Java usa: procesos.fnc_validar_botones_ventas_appterpel(idMovimiento)
# Retorna: pago (bool), fideliza (bool), proceso (bool)
# Si pago=true Y proceso=true → pago EN PROCESO, deshabilitar botones
# Si falla → pago=false o proceso=false → habilitar botones

@router.get("/appterpel-estado/{movimiento_id}")
async def validar_botones_appterpel(movimiento_id: int):
    """
    Consultar si una venta APP TERPEL permite gestión manual.
    
    Usa la misma función SQL que Java:
    procesos.fnc_validar_botones_ventas_appterpel(idMovimiento)
    
    Retorna:
    - pago_en_proceso: True si el pago está siendo procesado (NO tocar)
    - puede_gestionar: True si el pago falló y se puede asignar otro medio
    """
    try:
        print(f"[API] Validando botones AppTerpel para movimiento {movimiento_id}")
        
        query = "SELECT * FROM procesos.fnc_validar_botones_ventas_appterpel(:mov_id)"
        row = await database.fetch_one(query, {"mov_id": movimiento_id})
        
        if row:
            pago = bool(row['pago']) if 'pago' in row._mapping else True
            fideliza = bool(row['fideliza']) if 'fideliza' in row._mapping else True
            proceso = bool(row['proceso']) if 'proceso' in row._mapping else True
            
            # Java: if (validador.isPago() && validador.isProceso()) → deshabilitar
            pago_en_proceso = pago and proceso
            
            print(f"[API] AppTerpel estado: pago={pago}, fideliza={fideliza}, "
                  f"proceso={proceso}, en_proceso={pago_en_proceso}")
            
            return {
                "success": True,
                "movimiento_id": movimiento_id,
                "pago": pago,
                "fideliza": fideliza,
                "proceso": proceso,
                "pago_en_proceso": pago_en_proceso,
                "puede_gestionar": not pago_en_proceso
            }
        else:
            # Si no hay resultado, asumir que se puede gestionar (no hay proceso activo)
            print(f"[API] AppTerpel: sin resultado para movimiento {movimiento_id}, habilitando botones")
            return {
                "success": True,
                "movimiento_id": movimiento_id,
                "pago": True,
                "fideliza": True,
                "proceso": True,
                "pago_en_proceso": False,
                "puede_gestionar": True
            }
        
    except Exception as e:
        print(f"[API] Error validando AppTerpel: {e}")
        # Si hay error (ej: schema procesos no existe), habilitar por defecto
        return {
            "success": False,
            "movimiento_id": movimiento_id,
            "pago": True,
            "fideliza": True,
            "proceso": True,
            "pago_en_proceso": False,
            "puede_gestionar": True,
            "error": str(e)
        }


# ============================================================
# ASIGNAR APP TERPEL DESDE VENTAS SIN RESOLVER
# ============================================================
# Java flow (MedioPagosConfirmarViewController.java):
# 1. guardarMedioAppTerpel(): marca isAppTerpel=true en ventas_curso
# 2. sendMedioPago(): EXCLUYE APP TERPEL de fnc_actualizar_medios_de_pagos
# 3. Venta queda en estado 4 (pendiente) hasta que orquestador apruebe
# 4. Solo cuando es aprobado, se incluye APP TERPEL en fnc_actualizar_medios_de_pagos
#
# Este endpoint replica el paso 1-3: marca la venta como APP TERPEL pendiente
# SIN llamar fnc_actualizar_medios_de_pagos (eso gestiona la venta y la saca de pendientes)

class AsignarAppTerpelRequest(BaseModel):
    """Request para asignar APP TERPEL a una venta sin resolver"""
    movimiento_id: int
    medio_pago_id: int = 106  # ID de APP TERPEL en ct_medios_pagos
    medio_descripcion: str = "APP TERPEL"
    valor_total: float = 0

@router.post("/appterpel/asignar")
async def asignar_appterpel_venta(request: AsignarAppTerpelRequest):
    """
    Asignar APP TERPEL a una venta sin resolver SIN gestionarla.
    
    Replica el comportamiento de Java:
    - Marca isAppTerpel=true en atributos de ct_movimientos
    - Pone estado = 4 (pendiente, esperando pago)
    - NO llama a fnc_actualizar_medios_de_pagos (eso la gestiona/resuelve)
    - La venta sigue apareciendo en "ventas sin resolver"
    - Cuando el orquestador apruebe, ENTONCES se llama fnc_actualizar_medios_de_pagos
    """
    try:
        import json as json_lib
        
        print(f"[API] Asignando APP TERPEL a movimiento {request.movimiento_id} (sin gestionar)")
        
        # 1. Leer atributos actuales
        get_atributos = "SELECT atributos FROM ct_movimientos WHERE id = :mov_id"
        row = await database.fetch_one(get_atributos, {"mov_id": request.movimiento_id})
        
        if not row:
            return {
                "success": False,
                "message": f"No se encontró el movimiento {request.movimiento_id}",
                "movimiento_id": request.movimiento_id
            }
        
        atributos = {}
        if row['atributos']:
            if isinstance(row['atributos'], str):
                atributos = json_lib.loads(row['atributos'])
            else:
                atributos = dict(row['atributos'])
        
        # 2. Agregar isAppTerpel: true (igual que Java)
        # Java: atributos.addProperty("isAppTerpel", Boolean.TRUE)
        atributos['isAppTerpel'] = True
        
        atributos_json = json_lib.dumps(atributos)
        
        # 3. Actualizar ct_movimientos: atributos + estado pendiente (4)
        # Java: sincronizado = this.isAppTerpelPendiente ? 4 : 2
        # Estado 4 = pendiente (la venta sigue en "sin resolver")
        update_query = """
            UPDATE ct_movimientos 
            SET atributos = CAST(:atributos AS json),
                sincronizado = 4
            WHERE id = :mov_id
        """
        
        await database.execute(update_query, {
            "atributos": atributos_json,
            "mov_id": request.movimiento_id
        })
        
        print(f"[API] APP TERPEL asignado: isAppTerpel=true, sincronizado=4 (pendiente)")
        
        return {
            "success": True,
            "message": "APP TERPEL asignado - Esperando aprobación del orquestador",
            "movimiento_id": request.movimiento_id,
            "estado": "pendiente"
        }
        
    except Exception as e:
        print(f"[API] Error asignando APP TERPEL: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "movimiento_id": request.movimiento_id
        }


# ============================================================
# ENVIAR PAGO APP TERPEL AL ORQUESTADOR (puerto 5555)
# ============================================================
# Java: EnviandoMedioPago.java → POST http://localhost:5555/v1/payments/
# Request:  { identificadorMovimiento: long, medioDescription: string }
# Response: { IDSeguimiento, idTransaccion, estadoPago, technicalCode, mensaje }

class AppTerpelPagoRequest(BaseModel):
    """Request para enviar pago al orquestador"""
    movimiento_id: int
    medio_descripcion: str = "APP TERPEL"

@router.post("/appterpel/enviar-pago")
async def enviar_pago_appterpel(request: AppTerpelPagoRequest):
    """
    Envia el pago APP TERPEL al orquestador en puerto 5555.
    
    Replica el comportamiento de Java:
    - EnviandoMedioPago.java → POST http://localhost:5555/v1/payments/
    - PaymentRequest: { identificadorMovimiento, medioDescription }
    - PaymentResponse: { IDSeguimiento, idTransaccion, estadoPago, technicalCode, mensaje }
    """
    from app.url_global import ServiciosTerpel
    
    url_orquestador = ServiciosTerpel.url_orquestador_pagos()
    
    body_orquestador = {
        "identificadorMovimiento": request.movimiento_id,
        "medioDescription": request.medio_descripcion
    }
    
    try:
        print(f"[API] Enviando pago AppTerpel al orquestador: {url_orquestador}")
        print(f"[API] Body: {body_orquestador}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url_orquestador,
                json=body_orquestador,
                headers={"Content-Type": "application/json"}
            )
            
            print(f"[API] Respuesta orquestador: {response.status_code} - {response.text}")
            
            if 200 <= response.status_code < 300:
                data = response.json()
                return {
                    "success": True,
                    "message": "Pago enviado al orquestador",
                    "movimiento_id": request.movimiento_id,
                    "id_seguimiento": data.get("IDSeguimiento", ""),
                    "id_transaccion": data.get("idTransaccion", ""),
                    "estado_pago": data.get("estadoPago", ""),
                    "technical_code": data.get("technicalCode", 0),
                    "mensaje_orquestador": data.get("mensaje", "")
                }
            else:
                return {
                    "success": False,
                    "message": f"Error del orquestador: HTTP {response.status_code}",
                    "movimiento_id": request.movimiento_id,
                    "error": response.text
                }
                
    except httpx.TimeoutException:
        print(f"[API] Timeout llamando al orquestador: {url_orquestador}")
        return {
            "success": False,
            "message": "Timeout: El orquestador no respondió a tiempo",
            "movimiento_id": request.movimiento_id,
            "error": "TIMEOUT"
        }
    except httpx.ConnectError:
        print(f"[API] No se pudo conectar al orquestador: {url_orquestador}")
        return {
            "success": False,
            "message": "No se pudo conectar al orquestador (puerto 5555). Verifique que esté corriendo.",
            "movimiento_id": request.movimiento_id,
            "error": "CONNECTION_REFUSED"
        }
    except Exception as e:
        print(f"[API] Error enviando pago AppTerpel: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Error enviando pago: {str(e)}",
            "movimiento_id": request.movimiento_id,
            "error": str(e)
        }


# ============================================================
# CONSULTAR TIEMPO MENSAJE APP TERPEL
# ============================================================
# Java: NovusConstante.TIEMPO_MENSAJE_APPTERPEL (default 30s, configurable desde BD)

@router.get("/appterpel/tiempo-mensaje")
async def get_tiempo_mensaje_appterpel():
    """
    Obtiene el tiempo configurado para mostrar el mensaje de APP TERPEL.
    En Java se lee de la BD y el default es 30 segundos.
    """
    try:
        # Intentar leer de la tabla de parámetros (misma que usa Java)
        query = """
            SELECT valor FROM public.wacher_parametros 
            WHERE codigo = 'TIEMPO_MENSAJE_APPTERPEL'
        """
        row = await database.fetch_one(query)
        
        if row and row['valor']:
            tiempo = int(row['valor'])
            print(f"[API] Tiempo mensaje AppTerpel desde BD: {tiempo}s")
            return {"success": True, "tiempo_segundos": tiempo}
        else:
            # Default como en Java: NovusConstante.TIEMPO_MENSAJE_APPTERPEL = 30
            print("[API] Tiempo mensaje AppTerpel: usando default 30s")
            return {"success": True, "tiempo_segundos": 30}
            
    except Exception as e:
        print(f"[API] Error obteniendo tiempo AppTerpel: {e}")
        return {"success": True, "tiempo_segundos": 30}


# ============================================================
# IMPRESIÓN DE TICKET DE VENTA
# ============================================================
# Java: ImpresionVenta.impirmir() → POST http://localhost:8001/print-ticket/sales
# El servicio de impresión (puerto 8001) genera y envía el ticket a la impresora.

class ImprimirVentaRequest(BaseModel):
    movimiento_id: int
    report_type: str = "FACTURA"

@router.post("/imprimir")
async def imprimir_venta(req: ImprimirVentaRequest):
    """
    Envía la orden de impresión al servicio de impresión (puerto 8001).
    Java: ImpresionVenta.impirmir() → POST localhost:8001/print-ticket/sales
    Body: { movement_id, flow_type, report_type, body: {} }
    """
    try:
        url = "http://localhost:8001/print-ticket/sales"
        body = {
            "movement_id": req.movimiento_id,
            "flow_type": "CONSULTAR_VENTAS",
            "report_type": req.report_type.upper(),
            "body": {},
        }

        print(f"[API] Imprimiendo venta {req.movimiento_id} tipo={req.report_type}")
        print(f"[API] POST {url}")

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(url, json=body)

        print(f"[API] Print response: {response.status_code} - {response.text[:200]}")

        if response.status_code == 200:
            return {"exito": True, "mensaje": "Impresión enviada correctamente"}
        else:
            return {"exito": False, "mensaje": f"Error del servicio de impresión: HTTP {response.status_code}"}

    except httpx.ConnectError:
        print("[API] Servicio de impresión no disponible (puerto 8001)")
        return {"exito": False, "mensaje": "Servicio de impresión no disponible. Verifique que esté ejecutándose."}
    except Exception as e:
        print(f"[API] Error imprimiendo: {e}")
        return {"exito": False, "mensaje": f"Error: {str(e)}"}
