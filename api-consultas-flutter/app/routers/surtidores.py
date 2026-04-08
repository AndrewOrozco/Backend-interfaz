from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

from app.services.surtidores_service import SurtidoresService

router = APIRouter()

# ============================================================
# SCHEMAS
# ============================================================

class BloqueoItem(BaseModel):
    manguera: int
    bloqueo: bool
    motivo: str = ""

class BloqueoRequest(BaseModel):
    bloqueos: List[BloqueoItem]

# ============================================================
# ENDPOINTS
# ============================================================

class ArreglarSaltoRequest(BaseModel):
    configuracion_id: int

@router.post("/arreglar_salto")
async def arreglar_salto_lectura(req: ArreglarSaltoRequest):
    """
    Envía el comando al Core Gilbarco para limpiar un salto de lectura 
    para la manguera (identificada por configuracion_id).
    """
    try:
        exito = await SurtidoresService.arreglar_salto_lectura(req.configuracion_id)
        if exito:
            return {"exito": True, "mensaje": "Salto de lectura corregido exitosamente"}
        else:
            return {"exito": False, "mensaje": "No se pudo corregir el salto de lectura (falla de comunicación con el Core)"}
    except Exception as e:
        print(f"[SURTIDORES] Error en arreglar salto: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/mangueras")
async def obtener_mangueras_surtidores():
    """
    Obtiene el estado de las mangueras de los surtidores (surtidor, cara, manguera),
    incluyendo si están bloqueadas.
    """
    try:
        data = await SurtidoresService.obtener_mangueras()
        return {"exito": True, "data": data}
    except Exception as e:
        print(f"[SURTIDORES] Error obteniendo mangueras: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/bloqueo")
async def aplicar_bloqueos(req: BloqueoRequest):
    """
    Aplica múltiples bloqueos/desbloqueos de mangueras.
    """
    try:
        # Convert Pydantic objects to list of dicts
        bloqueos_dict = [item.dict() for item in req.bloqueos]
        exito = await SurtidoresService.actualizar_bloqueos(bloqueos_dict)
        
        if exito:
            return {"exito": True, "mensaje": "Bloqueos actualizados correctamente"}
        else:
            return {"exito": False, "mensaje": "Error actualizando estado en base de datos"}
            
    except Exception as e:
        print(f"[SURTIDORES] Error aplicando bloqueos: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

class TipoVentaRequest(BaseModel):
    surtidor: int
    cara: int
    manguera: int
    tipo_venta: int # 1=Predeterminado, 2=Calibracion, 3=Consumo propio
    monto: int = 0
    volumen: int = 0
    promotor_id: Optional[int] = None

@router.post("/tipo-venta")
async def crear_tipo_venta(req: TipoVentaRequest):
    """
    Crea una autorización especial (ej. Calibración) en transacciones.
    """
    try:
        exito = await SurtidoresService.crear_tipo_venta(
            surtidor=req.surtidor,
            cara=req.cara,
            manguera=req.manguera,
            tipo_venta=req.tipo_venta,
            promotor_id=req.promotor_id,
            monto=req.monto,
            volumen=req.volumen
        )
        if exito:
            return {"exito": True, "mensaje": "Autorización registrada correctamente"}
        else:
            return {"exito": False, "mensaje": "Error al registrar la autorización especial"}
    except Exception as e:
        print(f"[SURTIDORES] Error en crear_tipo_venta endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class CambioPrecioRequest(BaseModel):
    surtidor: int
    cara: int
    manguera: int
    nuevo_precio: int

@router.post("/cambio-precio")
async def aplicar_cambio_precio(req: CambioPrecioRequest):
    """
    Aplica el cambio de precio de una manguera por comandos.
    """
    try:
        exito = await SurtidoresService.aplicar_multicambioprecio(
            surtidor=req.surtidor,
            cara=req.cara,
            manguera=req.manguera,
            nuevo_precio=req.nuevo_precio
        )
        if exito:
            return {"exito": True, "mensaje": "Cambio de precio aplicado correctamente"}
        else:
            return {"exito": False, "mensaje": "Error de comunicación con Gilbarco Core al cambiar precio"}
    except Exception as e:
        print(f"[SURTIDORES] Error en cambio_precio endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/historial-remisiones")
async def historial_remisiones(registros: int = 50):
    """
    Obtiene el historial de remisiones de SAP.
    """
    try:
        data = await SurtidoresService.obtener_historial_remisiones(registros)
        return {"exito": True, "data": data}
    except Exception as e:
        print(f"[SURTIDORES] Error en historial_remisiones endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/remision/validar")
async def validar_remision(delivery: str):
    try:
        data = await SurtidoresService.validar_remision_sap(delivery)
        return {"exito": data["valido"], "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/remision/{delivery}/tanques")
async def obtener_tanques_remision(delivery: str):
    try:
        tanques = await SurtidoresService.obtener_tanques_remision(delivery)
        return {"exito": True, "data": tanques}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tanques-y-productos")
async def obtener_catalogos():
    """Devuelve los catálogos de Tanques y Productos para recepcion manual"""
    res = await SurtidoresService.obtener_tanques_y_productos_globales()
    return {"exito": True, "data": res}

@router.get("/aforo/{tanque_id}")
async def calcular_aforo_volumen(tanque_id: int, altura: float = 0.0):
    """
    Calcula el volumen aproximado para un tanque específico usando la tabla de aforos
    e interpolación lineal.
    """
    try:
        from app.database import database
        
        # Recuperar todos los puntos de aforo de este tanque guardados en ct_tabla_aforo
        query = "SELECT atributos::json->>'altura_valor' as altura, atributos::json->>'cantidad_valor' as cantidad FROM ct_tabla_aforo WHERE bodegas_id = :tanque_id;"
        puntos = await database.fetch_all(query=query, values={"tanque_id": tanque_id})
        
        if not puntos:
            return {"exito": False, "mensaje": "Sin aforo calibrado", "volumen": 0.0}
            
        puntos_procesados = []
        for p in puntos:
            try:
                puntos_procesados.append((float(p['altura']), float(p['cantidad'])))
            except:
                pass
                
        puntos_procesados.sort(key=lambda x: x[0])
        
        # Buscar en qué segmento cae la altura
        min_p = None
        max_p = None
        
        for p in puntos_procesados:
            if p[0] == altura:
                return {"exito": True, "volumen": p[1]}
            elif p[0] < altura:
                min_p = p
            elif p[0] > altura:
                max_p = p
                break
                
        if not min_p and max_p:
            return {"exito": True, "volumen": max_p[1]}
        elif not max_p and min_p:
            return {"exito": True, "volumen": min_p[1]}
        elif min_p and max_p:
            # Interpolación Lineal
            diff_altura = max_p[0] - min_p[0]
            diff_cantidad = max_p[1] - min_p[1]
            if diff_altura == 0:
                return {"exito": True, "volumen": min_p[1]}
                
            pendiente = diff_cantidad / diff_altura
            extra_altura = altura - min_p[0]
            volumen_calc = min_p[1] + (extra_altura * pendiente)
            return {"exito": True, "volumen": round(volumen_calc, 2)}
            
        return {"exito": False, "mensaje": "Aforo inválido", "volumen": 0.0}
        
    except Exception as e:
        print(f"[SURTIDORES] Error en aforo: {e}")
        return {"exito": False, "mensaje": str(e), "volumen": 0.0}

@router.post("/recepcion-combustible")
async def registrar_recepcion(body: dict):
    try:
        exito = await SurtidoresService.registrar_recepcion(body)
        return {"exito": exito, "mensaje": "Recepción de combustible procesada en el backend."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/recepciones/pendientes")
async def obtener_pendientes():
    try:
        data = await SurtidoresService.obtener_recepciones_pendientes()
        return {"exito": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
