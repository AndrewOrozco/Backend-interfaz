"""
Router para funcionalidad RUMBO (Gestión de Flotas)
====================================================
Orquesta la comunicación con los servicios existentes:
- Puerto 8010: Autorización de venta RUMBO (/v1.0/lazo-rumbo/admin/authorization)
- Puerto 8010: Datos adicionales (/v1.0/lazo-rumbo/admin/additional-data-multiple)
- Puerto 8014: Notificación estado RUMBO (/v1.0/lazo-rumbo/statechange)
- Base de datos: Mangueras desde surtidores_detalles + productos

Flujo simplificado (vs Java que tiene 6+ pantallas):
1. Flutter muestra pantalla única con: manguera + km + medio + identificador
2. Python orquesta todo: valida, autoriza con 8010, notifica 8014
3. Flutter recibe resultado: autorizado/rechazado
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from asyncio import Event as AsyncEvent
import json
import httpx
import asyncio
import time
from datetime import datetime

from app.database import database
from app.config import settings

router = APIRouter()

# Host del servidor central (LazoExpress). Normalmente localhost.
LAZO_HOST = getattr(settings, 'LAZO_HOST', 'localhost')

# ============================================================
# ALMACÉN DE LECTURAS DE IDENTIFICADORES EN MEMORIA
# ============================================================
# Cuando el Core envía una lectura de Ibutton/RFID,
# se almacena aquí. Flutter la consume vía polling.
# Estructura: { cara: { medio, serial, promotorId, promotorNombre, timestamp } }
_lecturas_pendientes: dict = {}
_lectura_events: dict = {}  # AsyncEvent por cara, para long-polling


# ============================================================
# SCHEMAS
# ============================================================

class MangueraInfo(BaseModel):
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
    es_urea: bool = False


class AutorizarRumboRequest(BaseModel):
    """Datos para solicitar autorización RUMBO.
    
    El servicio RUMBO (Node.js en puerto 8010) valida con Joi:
    - codigoTipoIdentificador: tipo de documento del promotor (1=Cédula, 2=Cédula Extranjería)
    - medioAutorizacion: medio de autorización (1=Tarjeta, 2=Ibutton, 3=RFID, 5=CódigoNumérico)
    - codigoFamiliaProducto: solo [1,3,5,6,7,8] (Corriente,Extra,Diesel,Gas,GLP,UREA)
    - serialIdentificador: min 5 chars, max 60
    - identificadorPromotor: >= 1 (optional, es la cédula del promotor)
    - idPromotor: >= 1 (optional, es el ID en DB)
    """
    surtidor: int
    cara: int
    manguera: int
    grado: int
    valor_odometro: int
    codigo_familia_producto: int  # Código familia: 1=Corriente, 3=Extra, 5=Diesel, 6=Gas, 7=GLP, 8=UREA
    precio_venta_unidad: float
    medio_autorizacion: int  # 1=Tarjeta, 2=Ibutton, 3=RFID, 5=CódigoNumérico
    serial_identificador: str  # Min 5 chars (serial del ibutton, rfid, tarjeta, o código numérico)
    codigo_seguridad: str = ""
    identificador_promotor: Optional[int] = None  # Cédula del promotor (>= 1)
    id_promotor: Optional[int] = None  # ID del promotor en DB (>= 1)
    codigo_tipo_identificador: int = 1  # 1=Cédula, 2=Cédula Extranjería (tipo documento promotor)
    codigo_producto: Optional[int] = None  # Solo para UREA/AdBlue


class AutorizarRumboResponse(BaseModel):
    """Respuesta de autorización RUMBO.
    
    timeout_autorizacion: segundos que tiene el promotor para bajar la manguera y despachar.
    timeout_datos_adicionales: segundos para completar datos adicionales (placa, pin).
    es_urea: si True, el flujo es AdBlue (sin countdown, sin espera de surtidor).
    litros_autorizados: solo para UREA, cantidad de litros que se puede despachar.
    """
    autorizado: bool
    mensaje: str
    identificador_autorizacion: Optional[str] = None
    placa_vehiculo: Optional[str] = None
    nombre_cliente: Optional[str] = None
    documento_cliente: Optional[str] = None
    es_urea: bool = False
    litros_autorizados: Optional[float] = None
    programa_cliente: Optional[str] = None
    monto_maximo: Optional[float] = None
    cantidad_maxima: Optional[float] = None
    requiere_datos_adicionales: bool = False
    requiere_placa: bool = False
    requiere_codigo_seguridad: bool = False
    timeout_autorizacion: Optional[int] = None  # Segundos para despachar
    timeout_datos_adicionales: Optional[int] = None  # Segundos para datos adicionales
    data_completa: Optional[dict] = None


class DatosAdicionalesRequest(BaseModel):
    """Datos adicionales post-autorización"""
    identificador_autorizacion: str
    placa: Optional[str] = None
    codigo_seguridad: Optional[str] = None
    informacion_adicional: Optional[str] = None


class NotificarEstadoRequest(BaseModel):
    """Notificación de cambio de estado RUMBO al puerto 8014"""
    identificador_autorizacion: str
    estado: str  # ej: "autorizado", "despachando", "finalizado"
    cara: int
    manguera: int


class ConfirmarUreaRequest(BaseModel):
    """Datos para confirmar venta UREA y registrar en ct_movimientos.
    
    Java: RumboView.insertInformacionMovimiento() → InsertCtMovimientosUseCase
    → VentasHistorialView.finalizarVentaAdBlue() → finalizacion-adblu (8014)
    → ActualizarCtMovimientosUseCase (actualiza cantidad y total)
    
    Flutter envía: request de autorización + response de Terpel + cantidad suministrada
    """
    # Datos del request de autorización
    surtidor: int = 1
    cara: int = 1
    valor_odometro: int
    codigo_familia_producto: int = 8
    precio_venta_unidad: float
    codigo_seguridad: str = ""
    codigo_tipo_identificador: int = 1
    serial_identificador: str
    medio_autorizacion: int
    identificador_grado: int = 0
    
    # Datos de la respuesta de Terpel
    data_completa: dict  # La respuesta completa del servicio RUMBO
    
    # Cantidad realmente suministrada (operador la ingresa)
    cantidad_suministrada: float = 0
    

# ============================================================
# ENDPOINTS
# ============================================================

@router.get("/mangueras", response_model=List[MangueraInfo])
async def obtener_mangueras():
    """
    Obtener mangueras disponibles para RUMBO.
    Replica la lógica de SetupDao.getMangueras() en Java.
    
    Consulta surtidores_detalles + productos + productos_familias
    donde estado_publico = 100 (disponible) y la cara no tiene
    ninguna manguera en estado diferente a 100.
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
            WHERE sur.cara NOT IN (
                SELECT cara 
                FROM surtidores_detalles sur2 
                INNER JOIN productos p2 ON p2.id = sur2.productos_id 
                INNER JOIN productos_familias pf2 ON p2.familias = pf2.id 
                WHERE sur2.estado_publico <> 100 
                GROUP BY 1
            )
            AND sur.estado_publico = 100
            ORDER BY sur.cara, sur.manguera
        """
        rows = await database.fetch_all(sql)
        
        mangueras = []
        for row in rows:
            bloqueado = False
            if row["bloqueo"] is not None:
                bloqueado = str(row["bloqueo"]).upper() == "S"
            
            mangueras.append(MangueraInfo(
                surtidor=int(row["surtidor"]),
                cara=int(row["cara"]),
                manguera=int(row["manguera"]),
                grado=int(row["grado"]),
                producto_id=int(row["productos_id"]),
                producto_descripcion=row["producto_descripcion"] or "N/A",
                producto_precio=float(row["producto_precio"] or 0),
                familia_id=int(row["familia_id"]),
                familia_descripcion=row["familia_descripcion"] or "N/A",
                bloqueado=bloqueado,
                motivo_bloqueo=row["motivo_bloqueo"],
                es_urea=False,
            ))
        
        # Verificar si UREA está habilitada
        urea_habilitada = await _verificar_integracion_urea()
        if urea_habilitada:
            urea_info = await _obtener_info_urea()
            if urea_info:
                mangueras.append(urea_info)
        
        print(f"[RUMBO] Mangueras disponibles: {len(mangueras)}")
        return mangueras
        
    except Exception as e:
        print(f"[RUMBO] Error obteniendo mangueras: {e}")
        raise HTTPException(status_code=500, detail=f"Error obteniendo mangueras: {str(e)}")


@router.get("/medios-identificacion")
async def obtener_medios_identificacion():
    """
    Obtener medios de identificación disponibles para RUMBO.
    Replica cargarMediosIdentificadores() de Java.
    
    Los medios son estáticos (hardcoded en Java también):
    - Ibutton (id=2): Requiere lector físico
    - RFID (id=3): Requiere lector físico  
    - Tarjeta (id=1): Entrada manual (serial + pass)
    - Código Numérico (id=5): Entrada manual
    """
    medios = [
        {
            "id": 2,
            "descripcion": "Ibutton",
            "requiere_lector": True,
            "icono": "vpn_key",
        },
        {
            "id": 3,
            "descripcion": "RFID",
            "requiere_lector": True,
            "icono": "nfc",
        },
        {
            "id": 1,
            "descripcion": "Tarjeta",
            "requiere_lector": False,
            "icono": "credit_card",
        },
        {
            "id": 5,
            "descripcion": "Código Numérico",
            "requiere_lector": False,
            "icono": "pin",
        },
    ]
    
    return {"medios": medios}


@router.post("/autorizar", response_model=AutorizarRumboResponse)
async def autorizar_venta_rumbo(request: AutorizarRumboRequest):
    """
    Solicitar autorización de venta RUMBO.
    
    Orquesta la comunicación con el servicio LazoExpress (puerto 8010):
    POST /v1.0/lazo-rumbo/admin/authorization
    
    Replica RumboFacade.fetchSaleAuthorization() + handleSaleAuthorizationResponse()
    
    CRITICO: Después de autorizar, actualiza ventas_curso con:
    - operario_id: ID del promotor activo
    Esto es necesario para que LazoExpress reconozca la venta como RUMBO.
    """
    try:
        # Validar que serialIdentificador tenga mínimo 5 caracteres (requisito del servicio RUMBO)
        serial = request.serial_identificador.strip()
        if len(serial) < 5:
            return AutorizarRumboResponse(
                autorizado=False,
                mensaje="El identificador debe tener mínimo 5 caracteres",
            )
        
        # CRITICO: Obtener el promotor activo desde la BD
        # En Java: Main.persona.getId() y Main.persona.getIdentificacion()
        # La BD tiene el personas.id REAL (ej: 9285). Flutter puede tener un
        # ID diferente (ej: SessionProvider devuelve un id de turno o persona
        # que NO coincide con personas.id). Por eso BD tiene PRIORIDAD.
        promotor_info = await _obtener_promotor_activo()
        
        # Determinar idPromotor e identificadorPromotor
        # PRIORIDAD: BD > request de Flutter > fallback
        # La BD siempre tiene el personas.id correcto (= Main.persona.getId() en Java)
        if promotor_info:
            id_promotor = promotor_info["id"]
            try:
                identificador_promotor = int(promotor_info["identificacion"])
            except (ValueError, TypeError):
                identificador_promotor = request.identificador_promotor
            print(f"[RUMBO] Usando promotor de BD: id={id_promotor} ident={identificador_promotor}")
        else:
            # Fallback a lo que envía Flutter
            id_promotor = request.id_promotor
            identificador_promotor = request.identificador_promotor
            print(f"[RUMBO] Sin promotor en BD, usando request: id={id_promotor} ident={identificador_promotor}")
        
        # Fallback final
        if id_promotor is None or id_promotor < 1:
            id_promotor = 1
            print("[RUMBO] ADVERTENCIA: No se encontró promotor activo, usando fallback id=1")
        
        # Tipo de identificación del promotor
        codigo_tipo_id = request.codigo_tipo_identificador
        if promotor_info and promotor_info.get("tipo_identificacion_id"):
            tipo_id_db = int(promotor_info["tipo_identificacion_id"])
            if tipo_id_db in (1, 2):
                codigo_tipo_id = tipo_id_db
        
        # Construir request para el servicio RUMBO (Node.js puerto 8010)
        # Campos requeridos según schema.ts del servicio RUMBO
        auth_request = {
            "surtidor": request.surtidor,
            "cantidad": 0,
            "monto": 0,
            "numeroCara": request.cara,
            "valorOdometro": request.valor_odometro,
            "codigoFamiliaProducto": request.codigo_familia_producto,
            "precioVentaUnidad": request.precio_venta_unidad,
            "codigoSeguridad": request.codigo_seguridad or "",
            "codigoTipoIdentificador": codigo_tipo_id,
            "serialIdentificador": serial,
            "medioAutorizacion": request.medio_autorizacion,
        }
        
        # IMPORTANTE: El servicio RUMBO usa estos campos directamente en SQL
        # (TransactionService.createBase). Si llegan como undefined en JS,
        # PostgreSQL falla con "no existe la columna «undefined»".
        # Por eso SIEMPRE debemos enviarlos, incluso con valores por defecto.
        
        # identificadorGrado: SIEMPRE enviar (Joi acepta min(0))
        auth_request["identificadorGrado"] = request.grado if request.grado is not None else 0
        
        # idPromotor: SIEMPRE enviar (Joi acepta min(1))
        # En Java: request.addProperty("idPromotor", Main.persona.getId())
        auth_request["idPromotor"] = id_promotor
        
        # identificadorPromotor: La cédula del promotor (Joi acepta min(1))
        # En Java: request.addProperty("identificadorPromotor", Main.persona.getIdentificacion())
        if identificador_promotor is not None and identificador_promotor >= 1:
            auth_request["identificadorPromotor"] = identificador_promotor
        
        # Detectar si es UREA/AdBlue: familia_id=8 (NovusConstante.CODIGO_FAMILIA_PRODUCTO_UREA)
        es_urea = (request.codigo_familia_producto == 8)
        
        # Para UREA: Java SIEMPRE envía codigoProducto = p_atributos->>'codigoExterno'
        # Terpel lo necesita para la autorización AdBlue
        if es_urea:
            codigo_externo_urea = await _obtener_codigo_externo_urea()
            if codigo_externo_urea:
                auth_request["codigoProducto"] = codigo_externo_urea
                print(f"[RUMBO-UREA] codigoProducto (externo) = {codigo_externo_urea}")
            elif request.codigo_producto:
                auth_request["codigoProducto"] = request.codigo_producto
                print(f"[RUMBO-UREA] codigoProducto (fallback producto_id) = {request.codigo_producto}")
        
        # Headers requeridos por el servicio RUMBO
        # IMPORTANTE: Replicar headers de Java para evitar que el proxy 8010
        # comprima la respuesta con gzip (Java no envía Accept-Encoding).
        headers = {
            "Content-Type": "application/json",
            "Authorization": "token",
            "aplicacion": "rumbopos",
            "identificadorDispositivo": "localhost",
            "fecha": datetime.now().isoformat(),
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept-Encoding": "identity",
        }
        
        if es_urea:
            # UREA va directo al puerto 8014 (servicio RUMBO) ruta /auth/
            # NO pasa por el proxy 8010. Java: NovusConstante.SECURE_CENTRAL_POINT_NOTIFICAR_AUTORIZACION_AD_BLUE
            # La ruta /auth/ en 8014 NO requiere JWT (a diferencia de /admin/ que sí lo necesita)
            url = f"http://{LAZO_HOST}:8014/v1.0/lazo-rumbo/auth/authorization-adblu"
            print(f"[RUMBO-UREA] POST {url}")
        else:
            # Combustible normal va al proxy 8010 que internamente se autentica con 8014
            url = f"http://{LAZO_HOST}:8010/v1.0/lazo-rumbo/admin/authorization"
            print(f"[RUMBO] POST {url}")
        
        print(f"[RUMBO] Request: {json.dumps(auth_request, indent=2)}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=auth_request, headers=headers)
        
        print(f"[RUMBO] Response status: {response.status_code}")
        
        # El servicio en 8010 puede responder con gzip sin Content-Encoding header.
        # httpx no lo descomprime automáticamente en ese caso.
        resp_json = {}
        raw_body = response.content
        try:
            resp_json = response.json()
            print(f"[RUMBO] Response JSON OK: {json.dumps(resp_json, ensure_ascii=False)[:500]}")
        except Exception:
            # Intentar descomprimir gzip manualmente
            import gzip
            try:
                decompressed = gzip.decompress(raw_body)
                resp_json = json.loads(decompressed.decode("utf-8"))
                print(f"[RUMBO] Response JSON (gzip): {json.dumps(resp_json, ensure_ascii=False)[:500]}")
            except Exception as gz_err:
                print(f"[RUMBO] Error parsing response: {gz_err}")
                print(f"[RUMBO] Raw bytes: {repr(raw_body[:100])}")
        
        if response.status_code == 200:
            # Formato exitoso del servicio RUMBO: { "mensaje": "...", "data": { ...AuthorizationRes... } }
            response_data = resp_json.get("data", resp_json)
            
            # Verificar si la respuesta tiene errores en el campo data
            errores = response_data.get("errores", {})
            if errores and (errores.get("codigoError", "") or errores.get("mensajeError", "")):
                msg = errores.get("mensajeError") or errores.get("codigoError") or "Error en autorización"
                return AutorizarRumboResponse(
                    autorizado=False,
                    mensaje=msg,
                )
            
            # Parsear respuesta exitosa
            datos_adicionales = response_data.get("datosAdicionales", {})
            timeouts = response_data.get("timeout", {})
            
            req_info_adicional = datos_adicionales.get("requiereInformacionAdicional", False)
            req_placa = datos_adicionales.get("requierePlacaVehiculo", False)
            req_codigo_seg = bool((datos_adicionales.get("codigoSeguridad") or "").strip())
            
            # Timeout en segundos (el servicio RUMBO los retorna en segundos)
            timeout_auth_sec = timeouts.get("authorization", 30)
            timeout_additional_sec = timeouts.get("additionalData", 30)
            
            # CRITICO: Actualizar ventas_curso.operario_id con el promotor real
            # Esto es necesario para que LazoExpress reconozca la venta como RUMBO.
            # En Java, Main.persona.getId() se usa para poblar ventas_curso.operario_id.
            # Sin esto, operario_id queda en 0 y LazoExpress no asocia la venta con RUMBO.
            if not es_urea:
                # Solo para combustible normal, UREA no tiene ventas_curso (es virtual)
                await _actualizar_operario_ventas_curso(
                    cara=request.cara,
                    operario_id=id_promotor,
                )
            
            # Para UREA: calcular litros autorizados
            # Java: litrosAutorizados = montoMaximo / precioUrea
            litros_urea = None
            if es_urea:
                monto_maximo = response_data.get("montoMaximo", 0)
                if monto_maximo and request.precio_venta_unidad > 0:
                    litros_urea = round(monto_maximo / request.precio_venta_unidad, 3)
                    print(f"[RUMBO-UREA] Litros autorizados: {litros_urea} (monto={monto_maximo} / precio={request.precio_venta_unidad})")
            
            return AutorizarRumboResponse(
                autorizado=True,
                mensaje=resp_json.get("mensaje", "Autorización aprobada"),
                identificador_autorizacion=str(response_data.get("identificadorAutorizacionEDS", "")),
                placa_vehiculo=response_data.get("placaVehiculo", ""),
                nombre_cliente=response_data.get("nombreCliente", "USUARIO APROBADO"),
                requiere_datos_adicionales=req_info_adicional,
                requiere_placa=req_placa,
                requiere_codigo_seguridad=req_codigo_seg,
                timeout_autorizacion=timeout_auth_sec,
                timeout_datos_adicionales=timeout_additional_sec,
                monto_maximo=response_data.get("montoMaximo"),
                cantidad_maxima=response_data.get("cantidadMaxima"),
                documento_cliente=response_data.get("documentoIdentificacionCliente", ""),
                programa_cliente=response_data.get("programaCliente", ""),
                data_completa=response_data,
                es_urea=es_urea,
                litros_autorizados=litros_urea,
            )
        else:
            # Formato de error del servicio RUMBO: { "codigoError": "...", "mensajeError": "...", "tipoError": "..." }
            msg_error = resp_json.get("mensajeError") or resp_json.get("mensaje") or f"HTTP {response.status_code}"
            codigo_error = resp_json.get("codigoError", "")
            detalle = f"[{codigo_error}] {msg_error}" if codigo_error else msg_error
            print(f"[RUMBO] ERROR: {detalle}")
            return AutorizarRumboResponse(
                autorizado=False,
                mensaje=detalle,
            )
            
    except httpx.ConnectError:
        return AutorizarRumboResponse(
            autorizado=False,
            mensaje="No se pudo conectar con el servicio de autorización (puerto 8010). Verifique que LazoExpress esté activo.",
        )
    except Exception as e:
        print(f"[RUMBO] Error en autorización: {e}")
        return AutorizarRumboResponse(
            autorizado=False,
            mensaje=f"Error interno: {str(e)}",
        )


@router.post("/datos-adicionales")
async def enviar_datos_adicionales(request: DatosAdicionalesRequest):
    """
    Enviar datos adicionales post-autorización.
    
    Cuando el servicio RUMBO (8010) responde que requiere datos adicionales
    (placa, código de seguridad, información adicional), este endpoint
    los envía al servicio.
    
    Replica RumboFacade.fetchAditionalData() en Java.
    """
    try:
        adicional_request = {
            "identificadorAutorizacionEDS": request.identificador_autorizacion,
        }
        
        if request.placa:
            adicional_request["placaVehiculo"] = request.placa
        if request.codigo_seguridad:
            adicional_request["codigoSeguridad"] = request.codigo_seguridad
        if request.informacion_adicional:
            adicional_request["informacionAdicional"] = request.informacion_adicional
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": "token",
            "aplicacion": "rumbopos",
            "identificadorDispositivo": "localhost",
            "fecha": datetime.now().isoformat(),
        }
        
        url = f"http://{LAZO_HOST}:8010/v1.0/lazo-rumbo/admin/additional-data-multiple"
        print(f"[RUMBO] POST datos-adicionales: {url}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=adicional_request, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("error") is not None:
                return {"exito": False, "mensaje": "Dato adicional inválido"}
            return {"exito": True, "mensaje": "Datos adicionales enviados correctamente"}
        else:
            return {"exito": False, "mensaje": f"Error HTTP {response.status_code}"}
            
    except httpx.ConnectError:
        return {"exito": False, "mensaje": "No se pudo conectar con el servicio (puerto 8010)"}
    except Exception as e:
        print(f"[RUMBO] Error datos adicionales: {e}")
        return {"exito": False, "mensaje": str(e)}


@router.get("/parametro-rumbo-activo")
async def verificar_parametro_rumbo():
    """
    Verificar si RUMBO está habilitado en la estación.
    Consulta wacher_parametros donde codigo = 'INTEGRACION_APP_RUMBO'.
    
    Nota: En Java esto está en EquipoDao.consultaParametroIntegracion()
    """
    try:
        sql = """
            SELECT coalesce(
                (SELECT valor FROM wacher_parametros WHERE codigo = 'INTEGRACION_APP_RUMBO'),
                'N'
            ) AS resultado
        """
        row = await database.fetch_one(sql)
        activo = row["resultado"] == "S" if row else False
        return {"rumbo_activo": activo}
    except Exception as e:
        print(f"[RUMBO] Error verificando parámetro: {e}")
        return {"rumbo_activo": False}


# ============================================================
# CONFIRMAR VENTA UREA (insertar en ct_movimientos)
# ============================================================

@router.post("/confirmar-urea")
async def confirmar_venta_urea(request: ConfirmarUreaRequest):
    """
    Confirmar venta UREA y registrar en ct_movimientos.
    
    Java: RumboView.insertInformacionMovimiento()
    → VentasCombustibleFacade.buildObjectCtMovimientos()
    → InsertCtMovimientosUseCase → fnc_insertar_ct_movimientos(7 JSON params)
    
    Esto crea la venta en ct_movimientos para que aparezca en "Ventas sin resolver".
    El operador luego la cierra manualmente cuando despacha la UREA.
    """
    try:
        print("[RUMBO-UREA] Confirmando venta UREA...")
        
        # Obtener datos necesarios de la BD
        promotor = await _obtener_promotor_activo()
        if not promotor:
            return {"exito": False, "mensaje": "No hay promotor activo"}
        
        equipo_info = await _obtener_equipo_info()
        jornada_id = await _obtener_jornada_id()
        bodega_urea_id = await _obtener_bodega_urea_id()
        producto_urea = await _obtener_info_producto_urea()
        
        if not producto_urea:
            return {"exito": False, "mensaje": "No se encontró producto UREA en la BD"}
        
        data = request.data_completa
        ahora = datetime.now().isoformat()
        fecha_ahora = datetime.now()
        
        # Calcular litros autorizados (Java: litrosAutorizados = montoMaximo / precioUrea)
        monto_maximo = data.get("montoMaximo", 0)
        precio_urea = producto_urea["precio"]
        cantidad_maxima = round(monto_maximo / precio_urea, 3) if precio_urea > 0 else 0
        
        # Agregar cantidadMaxima al data (Java lo hace antes de construir movimientos)
        data["cantidadMaxima"] = cantidad_maxima
        
        # --- 1. jsonMovimientos (VentasCombustibleFacade.buildObjectCtMovimientos con isCredito=false) ---
        json_movimientos = _build_ct_movimientos(
            data=data, request_data=request, promotor=promotor,
            equipo_info=equipo_info, jornada_id=jornada_id,
            fecha=ahora, fecha_dt=fecha_ahora,
            is_credito=False
        )
        
        # --- 2. jsonMovimientosCredito (isCredito=true) ---
        json_movimientos_credito = _build_ct_movimientos(
            data=data, request_data=request, promotor=promotor,
            equipo_info=equipo_info, jornada_id=jornada_id,
            fecha=ahora, fecha_dt=fecha_ahora,
            is_credito=True
        )
        
        # --- 3. jsonMovimientoDetalles ---
        # CRITICO: Keys en camelCase - Java usa Gson que serializa CTmovimientosDetallesBean así
        # La función PG lee con ->>'bodegasId', ->>'costoProducto', etc.
        json_detalles = {
            "movimientosId": 0,
            "bodegasId": bodega_urea_id or 0,
            "cantidad": 0,
            "costoProducto": 0,
            "precio": precio_urea,
            "descuentosId": 0,
            "descuentoCalculado": 0,
            "fecha": ahora,
            "ano": fecha_ahora.year,
            "mes": fecha_ahora.month,
            "dia": fecha_ahora.isoweekday(),
            "remotoId": 0,
            "sincronizado": "1",
            "subTotal": 0,
            "subMovimientosDetallesId": 0,
            "unidadesId": 1,
            "productosId": producto_urea["id"],
            "atributos": {}
        }
        
        # --- 4. jsonMovimientoMediosPago ---
        # CRITICO: Keys en camelCase - Java usa Gson que serializa CTmovimientoMediosPagoBean así
        json_medios_pago = {
            "ctMediosPagosId": 8,  # NovusConstante.ID_MEDIO_PAGO_CREDITO
            "ctMovimientosId": 0,
            "valorRecibido": 0,
            "valorCambio": 0,
            "valorTotal": 0,
            "numeroComprobante": "",
            "monedaLocal": "S",
            "trm": 0,
            "ingPagoDatafono": False
        }
        
        # --- 5. request JSON (la petición de autorización original) ---
        json_request = {
            "surtidor": request.surtidor,
            "cantidad": 0,
            "monto": 0,
            "numeroCara": request.cara,
            "valorOdometro": request.valor_odometro,
            "codigoFamiliaProducto": request.codigo_familia_producto,
            "precioVentaUnidad": request.precio_venta_unidad,
            "codigoSeguridad": data.get("codigoSeguridad", request.codigo_seguridad),
            "codigoTipoIdentificador": request.codigo_tipo_identificador,
            "serialIdentificador": request.serial_identificador,
            "medioAutorizacion": request.medio_autorizacion,
            "identificadorGrado": request.identificador_grado,
            "idPromotor": promotor["id"],
            "identificadorPromotor": int(promotor.get("identificacion", 0)),
        }
        
        # --- 6. response JSON (respuesta completa de Terpel con wrapper) ---
        json_response = {"data": data}
        
        # --- 7. identificacionAutorizacion ---
        json_ident_auth = {
            "identificadorAprobacion": data.get("identificadorAprobacion", ""),
            "identificadorAutorizacionEDS": data.get("identificadorAutorizacionEDS", ""),
        }
        
        # Ejecutar función PostgreSQL
        # IMPORTANTE: Usar CAST(:p AS json) en vez de :p::json
        # porque SQLAlchemy interpreta ::json como parámetro :json
        sql = """SELECT * FROM fnc_insertar_ct_movimientos(
            CAST(:p1 AS json), CAST(:p2 AS json), CAST(:p3 AS json),
            CAST(:p4 AS json), CAST(:p5 AS json), CAST(:p6 AS json), CAST(:p7 AS json)
        )"""
        
        print(f"[RUMBO-UREA] Llamando fnc_insertar_ct_movimientos...")
        print(f"[RUMBO-UREA] Promotor: {promotor['nombre']} (id={promotor['id']})")
        print(f"[RUMBO-UREA] Litros autorizados: {cantidad_maxima}")
        
        row = await database.fetch_one(
            query=sql,
            values={
                "p1": json.dumps(json_movimientos),
                "p2": json.dumps(json_movimientos_credito),
                "p3": json.dumps(json_detalles),
                "p4": json.dumps(json_medios_pago),
                "p5": json.dumps(json_request),
                "p6": json.dumps(json_response),
                "p7": json.dumps(json_ident_auth),
            }
        )
        
        resultado = bool(row[0]) if row else False
        
        if not resultado:
            print("[RUMBO-UREA] fnc_insertar_ct_movimientos retornó false")
            return {"exito": False, "mensaje": "Error al registrar la venta UREA"}
        
        print("[RUMBO-UREA] Venta UREA registrada en ct_movimientos")
        
        # La venta queda con cantidad=0 y venta_total=0
        # El operador la finaliza en "Ventas sin resolver" ingresando la cantidad real
        # Ahí se llama /finalizar-urea-sin-resolver que:
        #   1. Llama finalizacion-adblu (8014)
        #   2. Actualiza ct_movimientos con cantidad y total real
        
        return {
            "exito": True,
            "mensaje": "Venta UREA registrada - Cierre en Ventas sin resolver"
        }
            
    except Exception as e:
        print(f"[RUMBO-UREA] Error confirmando venta UREA: {e}")
        import traceback
        traceback.print_exc()
        return {"exito": False, "mensaje": f"Error: {str(e)}"}


@router.get("/detalles-urea/{movimiento_id}")
async def obtener_detalles_urea(movimiento_id: int):
    """
    Obtener detalles de una venta UREA desde ct_movimientos.
    
    Se usa en "Ventas sin resolver" para mostrar la pantalla de finalización
    con: placa, precio, litros autorizados, y permitir ingresar cantidad suministrada.
    
    Java: VentasHistorialView lee estos datos del modelo de la venta.
    """
    try:
        sql = """
            SELECT 
                cm.id,
                cm.venta_total,
                cm.atributos,
                cmd.precio
            FROM ct_movimientos cm
            LEFT JOIN ct_movimientos_detalles cmd ON cmd.movimientos_id = cm.id
            WHERE cm.id = :id
            LIMIT 1
        """
        row = await database.fetch_one(query=sql, values={"id": movimiento_id})
        
        if not row:
            return {"placa": "-", "precio": 0, "litros_autorizados": 0}
        
        precio = float(row["precio"] or 0)
        atributos = row["atributos"]
        
        # atributos puede ser str o dict (según driver)
        if isinstance(atributos, str):
            atributos = json.loads(atributos)
        elif atributos is None:
            atributos = {}
        
        placa = atributos.get("vehiculo_placa", "")
        if not placa:
            rumbo = atributos.get("rumbo", {})
            placa = rumbo.get("placaVehiculo", "-") if isinstance(rumbo, dict) else "-"
        
        # Litros autorizados: se guardó en extraData.response.data.cantidadMaxima
        # o se puede calcular de montoMaximo / precio
        extra_data = atributos.get("extraData", {})
        resp_data = {}
        if isinstance(extra_data, dict):
            resp = extra_data.get("response", {})
            if isinstance(resp, dict):
                resp_data = resp.get("data", {})
                if isinstance(resp_data, str):
                    resp_data = json.loads(resp_data)
        
        litros_autorizados = 0
        if isinstance(resp_data, dict):
            # Primero intentar cantidadMaxima calculada
            cant_max = resp_data.get("cantidadMaxima", 0)
            if cant_max and float(cant_max) > 0:
                litros_autorizados = float(cant_max)
            else:
                # Calcular desde montoMaximo / precio
                monto_max = resp_data.get("montoMaximo", 0)
                if monto_max and precio > 0:
                    litros_autorizados = round(float(monto_max) / precio, 3)
        
        print(f"[RUMBO-UREA] Detalles venta {movimiento_id}: placa={placa} precio={precio} litros={litros_autorizados}")
        
        return {
            "placa": placa,
            "precio": precio,
            "litros_autorizados": litros_autorizados,
        }
        
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo detalles UREA: {e}")
        return {"placa": "-", "precio": 0, "litros_autorizados": 0}


class FinalizarUreaSinResolverRequest(BaseModel):
    """Datos para finalizar venta UREA desde 'Ventas sin resolver'."""
    movimiento_id: int
    cantidad_suministrada: float
    precio_urea: float


@router.post("/finalizar-urea-sin-resolver")
async def finalizar_urea_sin_resolver(request: FinalizarUreaSinResolverRequest):
    """
    Finalizar venta UREA desde pantalla "Ventas sin resolver".
    
    Java: VentasHistorialView.finalizarVentaAdBlue()
    1. Llama finalizacion-adblu (8014)
    2. Actualiza ct_movimientos con cantidad y total real (ActualizarCtMovimientosUseCase)
    """
    try:
        mov_id = request.movimiento_id
        cantidad = request.cantidad_suministrada
        precio = request.precio_urea
        total_venta = cantidad * precio
        
        print(f"[RUMBO-UREA] Finalizando venta sin resolver #{mov_id}: cantidad={cantidad} precio={precio} total={total_venta}")
        
        # Obtener datos de la venta para construir request de finalización
        sql_mov = """
            SELECT cm.atributos FROM ct_movimientos cm WHERE cm.id = :id LIMIT 1
        """
        row = await database.fetch_one(query=sql_mov, values={"id": mov_id})
        
        if not row:
            return {"exito": False, "mensaje": f"Movimiento #{mov_id} no encontrado"}
        
        atributos = row["atributos"]
        if isinstance(atributos, str):
            atributos = json.loads(atributos)
        elif atributos is None:
            atributos = {}
        
        # Extraer datos necesarios para finalizacion-adblu
        extra_data = atributos.get("extraData", {})
        request_orig = extra_data.get("request", {}) if isinstance(extra_data, dict) else {}
        resp_orig = extra_data.get("response", {}) if isinstance(extra_data, dict) else {}
        data_orig = resp_orig.get("data", {}) if isinstance(resp_orig, dict) else {}
        if isinstance(data_orig, str):
            data_orig = json.loads(data_orig)
        
        rumbo = atributos.get("rumbo", {})
        if isinstance(rumbo, str):
            rumbo = json.loads(rumbo)
        
        promotor = await _obtener_promotor_activo()
        equipo_info = await _obtener_equipo_info()
        jornada_id = await _obtener_jornada_id()
        codigo_externo_urea = await _obtener_codigo_externo_urea()
        codigo_estacion = await _obtener_codigo_estacion()
        empresa_info = await _obtener_empresa_info()
        
        ahora = datetime.now().isoformat()
        
        # Construir request para finalizacion-adblu (8014)
        json_finalizacion = {
            "movimientoId": mov_id,
            "identificadorAutorizacionEDS": data_orig.get("identificadorAutorizacionEDS", rumbo.get("numeroRemisionLazo", "")),
            "identificadorAprobacion": data_orig.get("identificadorAprobacion", rumbo.get("identificadorAutorizacion", "")),
            "documentoIdentificacionCliente": data_orig.get("documentoIdentificacionCliente", rumbo.get("documentoIdentificacionCliente", "")),
            "programaCliente": data_orig.get("programaCliente", rumbo.get("programaCliente", "")),
            "precioUnidadCliente": precio,
            "codigoFamiliaProducto": 8,
            "precioUnidad": precio,
            "informacionAdicional": "1",
            "numeroCara": 1,
            "codigoIsla": str(equipo_info.get("isla", "1")),
            "numeroManguera": 1,
            "fechaInicioVenta": ahora,
            "fechaFinVenta": ahora,
            "numeroTicketeVenta": f"01{mov_id}",
            "serialIdentificador": request_orig.get("serialIdentificador", ""),
            "cantidadTotal": cantidad,
            "costoTotalCliente": total_venta,
            "costoTotalManguera": total_venta,
            "valorDescuentoCliente": 0,
            "valorIva": 0,
            "codigoProducto": codigo_externo_urea or 0,
            "identificadorTipoDocumentoPromotor": str(request_orig.get("codigoTipoIdentificador", 1)),
            "documentoIdentificacionPromotor": str(promotor.get("identificacion", "")) if promotor else "",
            "identificadorTurno": jornada_id,
            "numeroTurno": 1,
            "codigoEstacion": codigo_estacion,
            "nitEstacion": empresa_info.get("nit", ""),
            "nombreRegional": "REGIONAL",
            "nombreEstacion": empresa_info.get("alias", ""),
            "placaVehiculo": atributos.get("vehiculo_placa", rumbo.get("placaVehiculo", "")),
            "valorOdometro": str(atributos.get("vehiculo_odometro", "")),
            "lecturaInicial": 1,
            "lecturaFinal": 1,
        }
        
        # Llamar finalizacion-adblu en 8014
        try:
            url_fin = f"http://{LAZO_HOST}:8014/v1.0/lazo-rumbo/auth/finalizacion-adblu"
            headers_fin = {
                "Content-Type": "application/json",
                "Authorization": "token",
                "aplicacion": "rumbopos",
                "identificadorDispositivo": "localhost",
                "fecha": ahora,
            }
            print(f"[RUMBO-UREA] POST finalizacion-adblu: {url_fin}")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp_fin = await client.post(url_fin, json=json_finalizacion, headers=headers_fin)
            
            print(f"[RUMBO-UREA] Finalizacion status: {resp_fin.status_code}")
        except Exception as fin_err:
            print(f"[RUMBO-UREA] Error en finalizacion (no crítico): {fin_err}")
        
        # Actualizar ct_movimientos usando fnc_actualizar_ct_movimientos
        # Java: ActualizarCtMovimientosUseCase → fnc_actualizar_ct_movimientos(?::json)
        # Recibe: {movimientoId, cantidadSuministrada, totalVenta}
        # Esta función actualiza venta_total, cantidad en detalles, y cambia el estado
        # para que la venta aparezca en historial.
        json_actualizar = {
            "movimientoId": mov_id,
            "cantidadSuministrada": cantidad,
            "totalVenta": total_venta,
        }
        
        try:
            await database.fetch_all(
                query="SELECT * FROM fnc_actualizar_ct_movimientos(CAST(:data AS json))",
                values={"data": json.dumps(json_actualizar)}
            )
            print(f"[RUMBO-UREA] fnc_actualizar_ct_movimientos OK: id={mov_id} cantidad={cantidad} total={total_venta}")
        except Exception as fnc_err:
            print(f"[RUMBO-UREA] Error en fnc_actualizar_ct_movimientos: {fnc_err}")
            # Fallback: actualizar manualmente
            await database.execute(
                """UPDATE ct_movimientos 
                   SET venta_total = :total, 
                       atributos = jsonb_set(
                           COALESCE(atributos, '{}'::jsonb),
                           '{cantidadSuministrada}',
                           to_jsonb(:cantidad::numeric)
                       )
                   WHERE id = :id""",
                values={"total": total_venta, "cantidad": cantidad, "id": mov_id}
            )
            await database.execute(
                """UPDATE ct_movimientos_detalles 
                   SET cantidad = :cantidad, sub_total = :total, costo_producto = :precio
                   WHERE movimientos_id = :id""",
                values={"cantidad": cantidad, "total": total_venta, "precio": precio, "id": mov_id}
            )
        
        print(f"[RUMBO-UREA] Venta #{mov_id} finalizada: {cantidad} litros × ${precio} = ${total_venta}")
        
        return {
            "exito": True,
            "mensaje": f"Venta UREA finalizada: {cantidad} litros, total ${total_venta:.0f}",
        }
        
    except Exception as e:
        print(f"[RUMBO-UREA] Error finalizando UREA sin resolver: {e}")
        import traceback
        traceback.print_exc()
        return {"exito": False, "mensaje": f"Error: {str(e)}"}


def _build_ct_movimientos(
    data: dict, request_data: ConfirmarUreaRequest, promotor: dict,
    equipo_info: dict, jornada_id: str, fecha: str, fecha_dt: datetime,
    is_credito: bool
) -> dict:
    """Construir JSON de ct_movimientos.
    
    Replica VentasCombustibleFacade.buildObjectCtMovimientos() de Java.
    """
    mov_id = 1 if is_credito else 0
    
    # Atributos del movimiento
    atributos = {
        "tipo_negocio": 5,
        "responsables_nombre": promotor.get("nombre", ""),
        "responsables_identificacion": promotor.get("identificacion", ""),
        "personas_nombre": data.get("nombreCliente", ""),
        "personas_identificacion": data.get("documentoIdentificacionCliente", ""),
        "tercero_nombre": "",
        "tercero_identificacion": "",
        "precioDiferencial": {},
        "surtidor": 1,
        "cara": 1,
        "manguera": 1,
        "grado": 0,
        "islas": equipo_info.get("isla", "1"),
        "familiaDesc": "ADBLUE",
        "familiaId": 0,
        "consecutivo": None,
        "isElectronica": False,
        "suspendido": False,
        "cliente": {},
        "extraData": {
            "header": {},
            "request": {
                "surtidor": request_data.surtidor,
                "cantidad": 0,
                "monto": 0,
                "numeroCara": request_data.cara,
                "valorOdometro": request_data.valor_odometro,
                "codigoFamiliaProducto": request_data.codigo_familia_producto,
                "precioVentaUnidad": request_data.precio_venta_unidad,
                "codigoSeguridad": request_data.codigo_seguridad,
                "codigoTipoIdentificador": request_data.codigo_tipo_identificador,
                "serialIdentificador": request_data.serial_identificador,
                "medioAutorizacion": request_data.medio_autorizacion,
            },
            "response": {"data": data},
        },
        "fidelizada": "N",
        "vehiculo_placa": data.get("placaVehiculo", ""),
        "vehiculo_numero": " ",
        "vehiculo_odometro": str(data.get("valorOdometro", "")),
        "rumbo": {
            "programaCliente": data.get("programaCliente", ""),
            "identificadorTipoDocumentoCliente": data.get("identificadorTipoDocumentoCliente", 1),
            "documentoIdentificacionCliente": data.get("documentoIdentificacionCliente", ""),
            "nombreCliente": data.get("nombreCliente", ""),
            "identificadorFormaPago": data.get("identificadorFormaPago", 0),
            "codigoEstacion": data.get("codigoEstacion", ""),
            "placaVehiculo": data.get("placaVehiculo", ""),
            "identificadorAutorizacion": data.get("identificadorAprobacion", ""),
            "numeroRemisionLazo": data.get("identificadorAutorizacionEDS", ""),
            "numeroTicketeVenta": data.get("identificadorAutorizacionEDS", ""),
            "medio_autorizacion": str(request_data.medio_autorizacion),
            "tipoDescuentoCliente": 0,
            "valorDescuentoCliente": 0,
            "porcentajeDescuentoCliente": 0,
        },
        "CuentaLocal": None,
        "isCuentaLocal": False,
        "identificadorCupo": 0,
        "tipoCupo": None,
        "isCredito": True,
        "recuperada": False,
        "editarFidelizacion": True,
        "tipoVenta": 10 if is_credito else 4,
        "isContingencia": False,
    }
    
    if is_credito:
        atributos["motivoAnulacion"] = "CIENTES CREDITOS"
        atributos["tipoAnulacion"] = 1
    
    return {
        "empresas_id": equipo_info.get("empresas_id", 0),
        "tipo": "032" if is_credito else "017",       # TIPO_VENTA_COMBUSTIBLE / _CREDITO
        "estado_movimiento": "032001" if is_credito else "017006",  # MOV_VENTA_COMBUSTIBLE / _CREDITO
        "estado": "X",  # ESTADO_ESPECIAL_VENTA
        "fecha": fecha,
        "consecutivo": mov_id,
        "responsables_id": promotor["id"],
        "personas_id": 3 if is_credito else 2,
        "terceros_id": None,
        "costo_total": 0,
        "venta_total": 0,
        "impuesto_total": 0,
        "descuento_total": 0,
        "sincronizado": 0,
        "equipos_id": equipo_info.get("equipos_id", 0),
        "remoto_id": mov_id,
        "atributos": atributos,
        "impreso": "N",
        "movimientos_id": None if mov_id == 0 else mov_id,
        "uso_dolar": 0,
        "ano": fecha_dt.year,
        "mes": fecha_dt.month,
        "dia": fecha_dt.isoweekday(),
        "jornadas_id": jornada_id,
        "origen_id": None,
        "prefijo": "CREDITO" if is_credito else None,
        "id_tipo_venta": None,
        "json_data": None,
    }


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

async def _obtener_equipo_info() -> dict:
    """Obtener empresas_id y equipos_id desde la tabla equipos.
    Java: Main.credencial.getEmpresas_id() y Main.credencial.getEquipos_id()
    """
    try:
        sql = "SELECT id, empresas_id FROM equipos LIMIT 1"
        row = await database.fetch_one(sql)
        if row:
            return {
                "equipos_id": int(row["id"]),
                "empresas_id": int(row["empresas_id"]),
                "isla": "1",
            }
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo equipo: {e}")
    return {"equipos_id": 0, "empresas_id": 0, "isla": "1"}


async def _obtener_jornada_id() -> str:
    """Obtener ID de jornada activa.
    Java: ObtenerJornadaIdUseCase → SELECT j2.grupo_jornada FROM jornadas j2
    """
    try:
        sql = "SELECT grupo_jornada FROM jornadas WHERE fecha_fin IS NULL LIMIT 1"
        row = await database.fetch_one(sql)
        if row:
            return str(row["grupo_jornada"])
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo jornada: {e}")
    return ""


async def _obtener_bodega_urea_id() -> Optional[int]:
    """Obtener ID de bodega UREA (tipo 'V' = Virtual).
    Java: ObtenerIdBodegaUreaUseCase → SELECT cb.id FROM ct_bodegas cb WHERE atributos::json->>'tipo' = 'V'
    """
    try:
        sql = "SELECT cb.id FROM ct_bodegas cb WHERE cb.atributos::json->>'tipo' = 'V' LIMIT 1"
        row = await database.fetch_one(sql)
        if row:
            return int(row["id"])
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo bodega UREA: {e}")
    return None


async def _obtener_info_producto_urea() -> Optional[dict]:
    """Obtener ID y precio del producto UREA.
    Java: ObtenerIdProductoUreaUseCase + ObtenerPrecioUreaUseCase
    """
    try:
        sql = """
            SELECT p.id, p.precio, p.descripcion 
            FROM productos p 
            WHERE UPPER(p.descripcion) LIKE '%UREA%' 
            LIMIT 1
        """
        row = await database.fetch_one(sql)
        if row:
            return {
                "id": int(row["id"]),
                "precio": float(row["precio"] or 0),
                "descripcion": row["descripcion"],
            }
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo producto UREA: {e}")
    return None


async def _obtener_codigo_estacion() -> str:
    """Obtener código de estación desde wacher_parametros.
    Java: ObtenerCodigoEstacionUseCase -> EquipoDao.getCodigoEstacion()
    Consulta: SELECT valor FROM wacher_parametros WHERE codigo = 'codigoBackoffice'
    """
    try:
        sql = "SELECT valor FROM wacher_parametros WHERE codigo = 'codigoBackoffice' LIMIT 1"
        row = await database.fetch_one(sql)
        if row and row["valor"]:
            return str(row["valor"])
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo código estación: {e}")
    return ""


async def _obtener_empresa_info() -> dict:
    """Obtener info de la empresa (nit, alias, nombre)."""
    try:
        sql = "SELECT nit, alias, razon_social FROM empresas WHERE estado = 'A' LIMIT 1"
        row = await database.fetch_one(sql)
        if row:
            return {
                "nit": row["nit"] or "",
                "alias": row["alias"] or "",
                "razon_social": row["razon_social"] or "",
            }
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo empresa: {e}")
    return {"nit": "", "alias": "", "razon_social": ""}


async def _verificar_integracion_urea() -> bool:
    """Verificar si la integración UREA/AdBlue está habilitada.
    Java usa NovusConstante.PARAMETER_INTEGRACION_UREA = 'INTEGRACION_UREA' (mayúscula)
    """
    try:
        sql = """
            SELECT coalesce(
                (SELECT valor FROM wacher_parametros WHERE codigo = 'INTEGRACION_UREA'),
                'N'
            ) AS resultado
        """
        row = await database.fetch_one(sql)
        return row["resultado"] == "S" if row else False
    except Exception:
        return False




# ============================================================
# RECEPCIÓN DE LECTURAS DESDE EL CORE (Ibutton/RFID)
# ============================================================

class IdentificadorPromotorRequest(BaseModel):
    """Request que envía el Core Gilbarco al detectar un Ibutton/RFID.
    
    Mismo formato que el Core envía al Java POS (puerto 10000):
    POST /api/identificadorPromotor
    {medio, promotorId, promotorNombre, promotorIdentificador, cara}
    """
    medio: str  # "ibutton" o "rfid"
    promotorId: int = -1
    promotorNombre: str = ""
    promotorIdentificador: str = ""
    cara: int = 0


@router.post("/api/identificadorPromotor")
async def recibir_identificador_promotor(request: IdentificadorPromotorRequest):
    """
    Endpoint compatible con el Core Gilbarco.
    
    El Core lee un Ibutton o RFID y envía POST aquí con los datos.
    Almacenamos la lectura para que Flutter la consuma.
    
    Flujo:
      Core (hardware) → POST aquí → almacenar → Flutter poll → Flutter recibe
    
    Para configurar el Core, cambiar la URL del POST de:
      http://localhost:10000/api/identificadorPromotor  (Java POS)
    a:
      http://localhost:8020/rumbo/api/identificadorPromotor  (Python)
    """
    cara = request.cara
    lectura = {
        "medio": request.medio,
        "serial": request.promotorIdentificador,
        "promotor_id": request.promotorId,
        "promotor_nombre": request.promotorNombre,
        "cara": cara,
        "timestamp": time.time(),
    }
    
    _lecturas_pendientes[cara] = lectura
    print(f"[RUMBO] Lectura recibida del Core: cara={cara} medio={request.medio} serial={request.promotorIdentificador}")
    
    # Notificar a cualquier long-poll esperando en esta cara
    if cara in _lectura_events:
        _lectura_events[cara].set()
    
    return {
        "codigoEstacion": "0",
        "mensaje": "Lectura recibida",
        "cara": cara,
    }


@router.get("/lectura-identificador/{cara}")
async def obtener_lectura_identificador(cara: int, esperar: int = 0):
    """
    Flutter llama aquí para obtener la lectura pendiente de una cara.
    
    Parámetros:
    - cara: Número de cara del surtidor
    - esperar: Segundos a esperar si no hay lectura (long-polling, max 30s)
    
    Retorna la lectura pendiente o null si no hay.
    Una vez consumida, se elimina para evitar procesamiento duplicado.
    """
    # Si hay lectura disponible, retornar inmediatamente
    if cara in _lecturas_pendientes:
        lectura = _lecturas_pendientes.pop(cara)
        # Verificar que no sea muy vieja (máximo 60 segundos)
        if time.time() - lectura["timestamp"] < 60:
            return {"lectura": lectura, "disponible": True}
    
    # Si no hay lectura y esperar > 0, hacer long-polling
    if esperar > 0:
        esperar = min(esperar, 30)  # Máximo 30 segundos
        event = AsyncEvent()
        _lectura_events[cara] = event
        
        try:
            await asyncio.wait_for(event.wait(), timeout=esperar)
            # Evento disparado: hay una nueva lectura
            if cara in _lecturas_pendientes:
                lectura = _lecturas_pendientes.pop(cara)
                if time.time() - lectura["timestamp"] < 60:
                    return {"lectura": lectura, "disponible": True}
        except asyncio.TimeoutError:
            pass
        finally:
            _lectura_events.pop(cara, None)
    
    return {"lectura": None, "disponible": False}


@router.delete("/lectura-identificador/{cara}")
async def limpiar_lectura_identificador(cara: int):
    """Limpiar lectura pendiente de una cara (cancelar espera)."""
    _lecturas_pendientes.pop(cara, None)
    if cara in _lectura_events:
        _lectura_events[cara].set()
        _lectura_events.pop(cara, None)
    return {"mensaje": "Lectura limpiada"}


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

async def _obtener_info_urea() -> Optional[MangueraInfo]:
    """Obtener información del producto UREA para mostrar como manguera virtual.
    
    En Java: SetupDao.infoProductoUrea() crea un Surtidor virtual con:
    - surtidor=1, cara=1, manguera=99 (virtual)
    - familiaDescripcion="UREA", familiaIdentificador=8 (CODIGO_FAMILIA_PRODUCTO_UREA)
    - precio viene de ObtenerPrecioUreaUseCase (SELECT p.precio FROM productos WHERE descripcion = 'UREA')
    """
    try:
        # Buscar producto UREA por descripción (case insensitive, como Java)
        sql = """
            SELECT p.id, p.descripcion, p.precio, pf.id as familia_id
            FROM productos p
            LEFT JOIN productos_familias pf ON p.familias = pf.id
            WHERE UPPER(p.descripcion) LIKE '%UREA%'
            LIMIT 1
        """
        row = await database.fetch_one(sql)
        if row:
            return MangueraInfo(
                surtidor=1,
                cara=1,
                manguera=99,
                grado=0,
                producto_id=int(row["id"]),
                producto_descripcion=row["descripcion"] or "UREA",
                producto_precio=float(row["precio"] or 0),
                familia_id=int(row["familia_id"]) if row["familia_id"] else 8,
                familia_descripcion="UREA",
                bloqueado=False,
                es_urea=True,
            )
        else:
            print("[RUMBO] Producto UREA no encontrado en tabla productos")
    except Exception as e:
        print(f"[RUMBO] Error obteniendo info UREA: {e}")
    return None


async def _obtener_codigo_externo_urea() -> Optional[int]:
    """Obtener el código externo del producto UREA desde p_atributos JSON.
    
    Java: ObtenerCodigoExternoUreaUseCase → ProductoRepository.obtenerCodigoExternoUrea()
    SQL: SELECT COALESCE((SELECT (p.p_atributos::json->>'codigoExterno')::numeric 
          FROM productos p WHERE p.descripcion = ?), 0) AS codigo
    
    Terpel necesita este código para autorizar ventas AdBlue.
    """
    try:
        sql = """
            SELECT COALESCE(
                (SELECT (p.p_atributos::json->>'codigoExterno')::numeric 
                 FROM productos p 
                 WHERE UPPER(p.descripcion) LIKE '%UREA%' 
                 LIMIT 1),
                0
            ) AS codigo
        """
        row = await database.fetch_one(sql)
        if row and row["codigo"] and int(row["codigo"]) > 0:
            return int(row["codigo"])
        print("[RUMBO-UREA] No se encontró código externo de UREA en p_atributos")
    except Exception as e:
        print(f"[RUMBO-UREA] Error obteniendo código externo UREA: {e}")
    return None


async def _obtener_promotor_activo() -> Optional[dict]:
    """Obtener el promotor con turno/jornada activa.
    
    En Java: Main.persona contiene el usuario logueado con getId() e getIdentificacion().
    La jornada activa se identifica por fecha_fin IS NULL (no tiene columna 'estado').
    
    Retorna: {id, identificacion, nombre, tipo_identificacion_id} o None
    """
    try:
        sql = """
            SELECT 
                p.id,
                p.identificacion,
                p.nombre,
                p.tipos_identificacion_id AS tipo_identificacion_id
            FROM personas p
            INNER JOIN jornadas j ON j.personas_id = p.id
            WHERE j.fecha_fin IS NULL
            ORDER BY j.fecha_inicio DESC
            LIMIT 1
        """
        row = await database.fetch_one(sql)
        if row:
            info = {
                "id": int(row["id"]),
                "identificacion": str(row["identificacion"]),
                "nombre": row["nombre"] or "",
                "tipo_identificacion_id": int(row["tipo_identificacion_id"] or 1),
            }
            print(f"[RUMBO] Promotor activo: id={info['id']} "
                  f"ident={info['identificacion']} nombre={info['nombre']}")
            return info
        
        print("[RUMBO] No se encontró promotor con jornada activa")
        return None
    except Exception as e:
        print(f"[RUMBO] Error buscando promotor activo: {e}")
        return None


async def _actualizar_operario_ventas_curso(cara: int, operario_id: int):
    """Actualizar ventas_curso.operario_id para una cara.
    
    CRITICO para RUMBO: LazoExpress (8010) lee ventas_curso cuando procesa
    una venta. Si operario_id es 0, no puede asociar la venta al promotor RUMBO.
    
    En Java, Main.persona.getId() ya está en la sesión cuando el promotor abre RUMBO.
    Aquí lo escribimos explícitamente después de autorizar.
    
    NOTA: ventas_curso puede no tener fila aún para esta cara (la fila se crea
    cuando el surtidor empieza a despachar). Si no existe, es normal.
    El servicio RUMBO (8014) guarda el promotor_id en transacciones, y el
    middleware LazoExpress (8010) debería copiarlo a ventas_curso cuando la 
    bomba empiece. Si ya existe fila, la actualizamos como precaución.
    """
    try:
        result = await database.execute(
            "UPDATE ventas_curso SET operario_id = :operario WHERE cara = :cara",
            {"operario": operario_id, "cara": cara}
        )
        print(f"[RUMBO] ventas_curso.operario_id → cara={cara} operario={operario_id}")
    except Exception as e:
        print(f"[RUMBO] Info: ventas_curso update para cara={cara}: {e}")
