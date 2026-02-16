"""
Router para Fidelización (Club Terpel / Vive Terpel)
=====================================================
Permite acumular puntos de lealtad al vincular una cédula a una venta.

Flujo:
1. Validar cliente → GET 10462/lazo.loyalty/customer/validate/
2. Acumular puntos → POST 10462/lazo.loyalty/customer/accumulation/
3. Actualizar ct_movimientos.atributos.fidelizada = "S"

Java: LoyaltiesEndpoints.java, FidelizacionCliente.java, FidelizacionFacade.java
Puerto 10462 = proxy local lazo.loyalty (orquestador)
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import json
import httpx
from datetime import datetime

from app.database import database
from app.config import settings

router = APIRouter()

LAZO_HOST = getattr(settings, 'LAZO_HOST', 'localhost')

# Headers base para llamar a LazoExpress fidelización
FIDELIZACION_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Encoding": "identity",
    "Authorization": "Basic cGFzc3BvcnR4OlQ0MUFYUWtYSjh6",
    "aplicacion": "lazoexpress",
    "dispositivo": "proyectos",
}


# ============================================================
# SCHEMAS
# ============================================================

class ValidarClienteRequest(BaseModel):
    numero_identificacion: str
    codigo_tipo_identificacion: int = 1  # 1=CC, 2=CE, 3=Pasaporte, 4=LifeMiles


class AcumularPuntosRequest(BaseModel):
    movimiento_id: int
    numero_identificacion: str
    codigo_tipo_identificacion: int = 1
    # Datos de la venta (se obtienen del movimiento si no se pasan)
    prefijo: Optional[str] = None
    consecutivo: Optional[int] = None
    total_venta: Optional[float] = None
    productos: Optional[List[dict]] = None
    medios_pago: Optional[List[dict]] = None


# ============================================================
# ENDPOINTS
# ============================================================

@router.post("/validar-cliente")
async def validar_cliente(req: ValidarClienteRequest):
    """
    Validar si un cliente existe en el sistema de lealtad de Terpel.
    Java: ConsultarClienteHandler → GET 10462/lazo.loyalty/customer/validate/
    """
    try:
        # Obtener código de estación
        estacion_row = await database.fetch_one(
            "SELECT codigo_empresa FROM empresas LIMIT 1"
        )
        codigo_estacion = estacion_row["codigo_empresa"] if estacion_row else "EDS1"

        # Mapear codigo_tipo_identificacion numerico a código fidelización
        # Java: RenderizarProcesosFidelizacionyFE.tiposIdentificaionFidelizaicon
        tipo_fid_map = {1: "CC", 2: "CE", 3: "PAS"}
        tipo_fid = tipo_fid_map.get(req.codigo_tipo_identificacion, "CC")

        # Puerto 10462 espera ConsultClientRequestBody (transactionData + customer)
        body = {
            "transactionData": {
                "origenVenta": "EDS",
                "identificacionPuntoVenta": codigo_estacion,
                "fechaTransaccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
            "customer": {
                "codigoTipoIdentificacion": tipo_fid,
                "numeroIdentificacion": req.numero_identificacion,
            },
            "idIntegracion": None,
        }

        # Java usa puerto 10462 (proxy local lazo.loyalty), NO 8010
        # LoyaltiesEndpoints.CONSULTARCLIENTE = "http://127.0.0.1:10462/lazo.loyalty/customer/validate/"
        url = "http://127.0.0.1:10462/lazo.loyalty/customer/validate/"
        print(f"[FIDELIZACION] Validando cliente: {req.numero_identificacion}")
        print(f"[FIDELIZACION] GET {url}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                "GET",
                url,
                json=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

        print(f"[FIDELIZACION] Validar response status: {response.status_code}")

        if response.status_code != 200:
            texto = response.text[:500]
            print(f"[FIDELIZACION] Error: {texto}")
            return {
                "exito": False,
                "mensaje": f"Error del servicio: HTTP {response.status_code}",
            }

        data = response.json()
        print(f"[FIDELIZACION] Validar response: {json.dumps(data, indent=2, default=str)[:500]}")

        # Puerto 10462 devuelve FoundClient: {nombreCliente, existeClient, mensaje, status, datosCliente}
        nombre = data.get("nombreCliente", "")
        existe = data.get("existeClient", False)
        mensaje = data.get("mensaje", data.get("mensajeRespuesta", ""))

        if existe and nombre and nombre not in ("NO REGISTRADO", "CLIENTE", ""):
            return {
                "exito": True,
                "cliente": {
                    "nombre": nombre,
                    "numero_identificacion": req.numero_identificacion,
                    "codigo_tipo_identificacion": req.codigo_tipo_identificacion,
                },
                "mensaje": mensaje,
            }
        else:
            return {
                "exito": False,
                "mensaje": mensaje or "Cliente no encontrado en el programa de fidelización",
            }

    except httpx.ConnectError:
        return {"exito": False, "mensaje": "Sin conexión al servicio de fidelización"}
    except Exception as e:
        print(f"[FIDELIZACION] Error validando cliente: {e}")
        return {"exito": False, "mensaje": str(e)}


@router.post("/acumular")
async def acumular_puntos(req: AcumularPuntosRequest):
    """
    Acumular puntos de lealtad para un cliente en una venta.
    Java: AcomulacionPuntosHandler → POST 8010/v2.0/lazo.fidelizacion/acumulacionCliente
    Luego actualiza ct_movimientos.atributos.fidelizada = "S"
    """
    try:
        # Obtener datos de la estación
        estacion_row = await database.fetch_one(
            "SELECT codigo_empresa FROM empresas LIMIT 1"
        )
        codigo_estacion = estacion_row["codigo_empresa"] if estacion_row else "EDS1"

        # Obtener datos del promotor activo
        promotor_row = await database.fetch_one(
            """SELECT p.id, p.identificacion 
               FROM jornadas j 
               INNER JOIN personas p ON p.id = j.personas_id 
               WHERE j.fecha_fin IS NULL 
               ORDER BY j.fecha_inicio DESC LIMIT 1"""
        )
        id_promotor = str(promotor_row["identificacion"]) if promotor_row else "0"

        # Obtener datos del movimiento si no se pasan
        mov_row = await database.fetch_one(
            """SELECT m.id, m.prefijo, m.consecutivo, m.venta_total, m.atributos,
                      md.productos_id, md.cantidad as cantidad_venta, 
                      md.precio as precio_producto, md.sub_total
               FROM ct_movimientos m
               LEFT JOIN ct_movimientos_detalles md ON md.movimientos_id = m.id
               WHERE m.id = :mid
               LIMIT 1""",
            values={"mid": req.movimiento_id}
        )

        if not mov_row:
            return {"exito": False, "mensaje": "Movimiento no encontrado"}

        prefijo = req.prefijo or mov_row["prefijo"] or ""
        consecutivo = req.consecutivo or mov_row["consecutivo"] or 0
        total_venta = req.total_venta or float(mov_row["venta_total"] or 0)

        # Obtener todos los detalles del movimiento
        detalles_rows = await database.fetch_all(
            """SELECT md.productos_id, md.cantidad, md.precio, md.sub_total,
                      p.descripcion as producto_desc
               FROM ct_movimientos_detalles md
               LEFT JOIN productos p ON p.id = md.productos_id
               WHERE md.movimientos_id = :mid""",
            values={"mid": req.movimiento_id}
        )

        # Mapear productos → ProductsLoyalty (Java: identificacionProducto, cantidadProducto, valorUnitarioProducto, lineaNegocio)
        productos = []
        for det in detalles_rows:
            productos.append({
                "identificacionProducto": str(det["productos_id"] or "0"),
                "cantidadProducto": float(det["cantidad"] or 0),
                "valorUnitarioProducto": round(float(det["precio"] or 0)),
                "lineaNegocio": None,
            })

        if not productos:
            productos = [{"identificacionProducto": "0", "cantidadProducto": 0, "valorUnitarioProducto": 0, "lineaNegocio": None}]

        # Obtener medios de pago
        medios_rows = await database.fetch_all(
            """SELECT mp.id, mpm.valor_total, mpm.valor_recibido
               FROM ct_movimientos_medios_pagos mpm
               INNER JOIN medios_pagos mp ON mp.id = mpm.ct_medios_pagos_id
               WHERE mpm.ct_movimientos_id = :mid""",
            values={"mid": req.movimiento_id}
        )

        # Mapear → MediosPagoLoyalty (Java: identificacionMedioPago, valorPago como strings)
        medios_pago = []
        for mp in medios_rows:
            valor_recibido = float(mp["valor_recibido"] or mp["valor_total"] or 0)
            medios_pago.append({
                "identificacionMedioPago": str(mp["id"]),
                "valorPago": str(round(valor_recibido)),
            })

        if not medios_pago:
            medios_pago = [{"identificacionMedioPago": "1", "valorPago": str(round(total_venta))}]

        # Construir request exactamente como Java: AcumulationLoyaltyRequestBody
        # Java: AcumularPuntosService.execute() → wraps in datosAcumulacion + IdIntegration
        identificacion_venta = f"{prefijo}-{consecutivo}" if prefijo else str(consecutivo)
        fecha_tx = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Mapear tipo documento numérico a código string (Java: tiposIdentificaionFidelizaicon)
        tipo_fid_map = {1: "CC", 2: "CE", 3: "PAS"}
        tipo_fid = tipo_fid_map.get(req.codigo_tipo_identificacion, "CC")

        body = {
            "datosAcumulacion": {
                "identificacionCliente": {
                    "codigoTipoIdentificacion": tipo_fid,
                    "numeroIdentificacion": req.numero_identificacion,
                },
                "productos": productos,
                "mediosPago": medios_pago,
                "transactionData": {
                    "origenVenta": "EDS",
                    "identificacionPuntoVenta": codigo_estacion,
                    "fechaTransaccion": fecha_tx,
                },
                "salesData": {
                    "fechaTransaccion": fecha_tx,
                    "identificacionPuntoVenta": codigo_estacion,
                    "origenVenta": "EDS",
                    "tipoVenta": "L",
                    "identificacionPromotor": id_promotor,
                    "identificacionVenta": identificacion_venta,
                    "valorTotalVenta": round(total_venta),
                    "totalImpuesto": 0,
                    "descuentoVenta": 0,
                    "pagoTotal": round(total_venta),
                    "movimientoId": req.movimiento_id,
                },
            },
            "IdIntegration": 3,
        }

        # Java usa puerto 10462 (proxy local lazo.loyalty), NO 8010
        # LoyaltiesEndpoints.ACUMULARPUNTOS = "http://127.0.0.1:10462/lazo.loyalty/customer/accumulation/"
        url = "http://127.0.0.1:10462/lazo.loyalty/customer/accumulation/"
        print(f"[FIDELIZACION] Acumulando puntos para {req.numero_identificacion} en mov {req.movimiento_id}")
        print(f"[FIDELIZACION] POST {url}")
        print(f"[FIDELIZACION] Body: {json.dumps(body, indent=2, default=str)[:1000]}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                json=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

        print(f"[FIDELIZACION] Acumular response status: {response.status_code}")

        if response.status_code != 200:
            texto = response.text[:500]
            print(f"[FIDELIZACION] Error: {texto}")
            return {"exito": False, "mensaje": f"Error del servicio: HTTP {response.status_code}"}

        data = response.json()
        print(f"[FIDELIZACION] Acumular response: {json.dumps(data, indent=2, default=str)[:500]}")

        # Puerto 10462 devuelve RespuestasAcumulacion: {codigoRespuesta, mensajeRespuesta, status, ...}
        codigo = str(data.get("codigoRespuesta", ""))
        mensaje = data.get("mensajeRespuesta", data.get("mensaje", ""))
        status_resp = data.get("status", 0)

        acumulacion_ok = codigo == "20000" or (response.status_code == 200 and status_resp in (0, 200))
        if acumulacion_ok:
            # Actualizar ct_movimientos.atributos con fidelizada = "S"
            try:
                atributos_raw = mov_row["atributos"]
                if atributos_raw:
                    atributos = json.loads(atributos_raw) if isinstance(atributos_raw, str) else dict(atributos_raw)
                else:
                    atributos = {}

                atributos["fidelizada"] = "S"
                atributos["editarFidelizacion"] = False

                await database.execute(
                    "UPDATE ct_movimientos SET atributos = CAST(:attr AS json) WHERE id = :mid",
                    values={"attr": json.dumps(atributos), "mid": req.movimiento_id}
                )
                print(f"[FIDELIZACION] ct_movimientos {req.movimiento_id} actualizado: fidelizada=S")
            except Exception as db_err:
                print(f"[FIDELIZACION] Error actualizando BD: {db_err}")

            return {
                "exito": True,
                "mensaje": mensaje or "Puntos acumulados correctamente",
                "data": data,
            }
        else:
            return {
                "exito": False,
                "codigo": codigo,
                "mensaje": mensaje or "Error al acumular puntos",
            }

    except httpx.ConnectError:
        return {"exito": False, "mensaje": "Sin conexión al servicio de fidelización"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[FIDELIZACION] Error acumulando puntos: {e}")
        return {"exito": False, "mensaje": str(e)}


@router.get("/estado/{movimiento_id}")
async def obtener_estado_fidelizacion(movimiento_id: int):
    """
    Verificar si una venta ya fue fidelizada.
    """
    try:
        row = await database.fetch_one(
            "SELECT atributos FROM ct_movimientos WHERE id = :mid",
            values={"mid": movimiento_id}
        )
        if not row:
            return {"fidelizada": False}

        atributos = row["atributos"]
        if atributos:
            attr = json.loads(atributos) if isinstance(atributos, str) else dict(atributos)
            fidelizada = attr.get("fidelizada", "N") == "S"
            return {"fidelizada": fidelizada}

        return {"fidelizada": False}
    except Exception as e:
        print(f"[FIDELIZACION] Error consultando estado: {e}")
        return {"fidelizada": False}
