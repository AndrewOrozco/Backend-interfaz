"""
Router FastAPI para integrar en api-consultas-flutter.
- POST /ventas/enviar-fe-pump: venta gestionada desde Flutter (status pump) → enviar a 7011 e imprimir.
- POST /ventas/sin-resolver/resolver-y-enviar-fe: resolver venta sin resolver → enviar a 7011 de inmediato, opcional imprimir.

Usa URL_GLOBAL (host desde BD wacher_parametros) para construir el endpoint del 7011.

Integración con tabla ``transmision`` (lazoexpressregistry):
- INSERT antes de enviar al 7011
- UPDATE con respuesta después
- Enriquece payload con pagos desde ct_movimientos_medios_pagos
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Si backend_fe_7011 está en la raíz del proyecto (recomendado):
try:
    from backend_fe_7011.fe_flow import enviar_a_7011_y_opcionalmente_imprimir, enviar_venta_sin_resolver_a_7011
    from backend_fe_7011.fe_7011_client import FE7011Client, ENDPOINT_ENVIAR_FE
    from backend_fe_7011.fe_transmision import (
        obtener_equipo_id,
        insertar_transmision,
        actualizar_transmision_respuesta,
        actualizar_request_transmision,
        obtener_pagos_movimiento,
    )
except ImportError:
    # Fallback si ejecutas desde dentro de backend_fe_7011
    import sys
    from pathlib import Path
    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    from fe_flow import enviar_a_7011_y_opcionalmente_imprimir, enviar_venta_sin_resolver_a_7011
    from fe_7011_client import FE7011Client, ENDPOINT_ENVIAR_FE
    from fe_transmision import (
        obtener_equipo_id,
        insertar_transmision,
        actualizar_transmision_respuesta,
        actualizar_request_transmision,
        obtener_pagos_movimiento,
    )

# Para construir la URL del 7011 con el host de la BD (URL_GLOBAL)
try:
    from app.database import database, database_registry
    from app.url_global import get_host_from_db, ServiciosTerpel, consultar_cliente_externo
except ImportError:
    database = None
    database_registry = None
    get_host_from_db = None
    ServiciosTerpel = None
    consultar_cliente_externo = None


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ventas", tags=["facturacion-electronica-7011"])


class EnviarFEPumpBody(BaseModel):
    """Payload cuando Flutter gestiona la venta pump y envía para FE + impresión."""
    payload_fe: dict[str, Any]  # JSON venta + cliente (como lo arma el backend o Flutter)
    identificador_movimiento: int
    imprimir_despues: bool = True
    tipo_reporte: str = "FACTURA-ELECTRONICA"


class ResolverYEnviarFEBody(BaseModel):
    """Payload al resolver una venta sin resolver (asignar cliente y enviar a FE)."""
    payload_fe: dict[str, Any]
    identificador_movimiento: int
    imprimir_despues: bool = False  # desde historial el usuario puede imprimir después


async def _fe_client_from_url_global():
    """Construye FE7011Client usando el host de URL_GLOBAL (BD)."""
    if database is None or get_host_from_db is None or ServiciosTerpel is None:
        return FE7011Client()
    host = await get_host_from_db(database)
    base_url = ServiciosTerpel.url_base_7011(host)
    logger.info("[FE-7011] Usando URL_GLOBAL para 7011: %s", base_url)
    return FE7011Client(base_url=base_url)


async def _enriquecer_payload_con_pagos(payload: dict, movimiento_id: int) -> dict:
    """
    Agrega ``pagos`` al payload FE con los medios de pago del movimiento.
    Similar a como Java incluye ``pagos`` en el request de transmision.
    """
    if database is None:
        return payload
    if "pagos" not in payload:
        pagos = await obtener_pagos_movimiento(database, movimiento_id)
        if pagos:
            payload["pagos"] = pagos
            logger.info("[FE-7011] Payload enriquecido con %d medio(s) de pago", len(pagos))
    return payload


async def _registrar_transmision(fe_client: FE7011Client, payload: dict, movimiento_id: int = 0) -> int:
    """
    INSERT en tabla ``transmision`` de lazoexpressregistry antes de enviar a 7011.
    Si ya existe una transmisión para este movimiento (hoy), reutiliza ese id.
    Retorna id_transmision (0 si falla o no hay database_registry).
    """
    if database_registry is None:
        return 0
    try:
        # ── Deduplicación: buscar si ya existe para este movimiento ──
        mov_id_str = str(movimiento_id or payload.get("identificadorMovimiento") or
                        (payload.get("venta") or {}).get("identificadorMovimiento") or 0)
        if mov_id_str and mov_id_str != "0":
            existing = await database_registry.fetch_one(
                """SELECT id FROM transmision
                   WHERE request::json->>'identificadorMovimiento' = :mov
                     AND fecha_generado >= CURRENT_DATE
                   ORDER BY id DESC LIMIT 1""",
                {"mov": mov_id_str},
            )
            if existing:
                logger.info("[FE-7011] Transmision ya existe id=%s para mov=%s, reutilizando", existing["id"], mov_id_str)
                return int(existing["id"])

        equipo_id = await obtener_equipo_id(database_registry)
        url_7011 = f"{fe_client.base_url}{ENDPOINT_ENVIAR_FE}"
        id_transmision = await insertar_transmision(
            database_registry,
            equipo_id=equipo_id,
            request_payload=payload,
            url=url_7011,
            method="POST",
        )
        return id_transmision
    except Exception as e:
        logger.warning("[FE-7011] No se pudo registrar transmision: %s", e)
        return 0


async def _actualizar_transmision(
    id_transmision: int,
    result: dict,
    payload_enviado: dict | None = None,
) -> None:
    """
    UPDATE tabla ``transmision`` después de enviar a 7011.
    - Si OK: sincronizado=1, status=200
    - Si error: sincronizado=0, status=409
    También actualiza request con payload enriquecido (datos de cliente).
    """
    if database_registry is None or id_transmision <= 0:
        return
    try:
        # 1) Actualizar respuesta y estado
        if result.get("error_7011"):
            await actualizar_transmision_respuesta(
                database_registry, id_transmision,
                status_code=409,
                response_data={"error": result["error_7011"]},
            )
        else:
            await actualizar_transmision_respuesta(
                database_registry, id_transmision,
                status_code=200,
                response_data=result.get("response_7011"),
            )

        # 2) Actualizar request con datos enriquecidos (cliente + tercero)
        #    Equivalente a ActualizarAtributosTransmisionUseCase en Java
        final_payload = payload_enviado or result.get("payload_enviado")
        if final_payload:
            await actualizar_request_transmision(
                database_registry, id_transmision, final_payload,
            )
    except Exception as e:
        logger.warning("[FE-7011] Error actualizando transmision id=%s: %s", id_transmision, e)


@router.post("/enviar-fe-pump")
async def enviar_fe_pump(body: EnviarFEPumpBody):
    """
    Llamar cuando la venta fue gestionada desde Flutter (status pump).
    Envía a 7011 y, si imprimir_despues, dispara la impresión de la factura ya enviada.
    Usa el host de URL_GLOBAL (wacher_parametros) para el endpoint 7011.

    ⚠️ Si el medio de pago es AppTerpel o GoPass, NO envía al 7011.
       El envío se hace después, cuando el orquestador confirma APROBADO
       y llama a /sin-resolver/resolver-y-enviar-fe.

    Integración transmision (Java-compatible):
    1. Enriquece payload con pagos
    2. INSERT en transmision antes de enviar
    3. Envía a 7011
    4. UPDATE transmision con respuesta
    """
    try:
        from app.ws_notifications import notification_hub

        # ── Guard: NO enviar si es AppTerpel/GoPass (esperar callback orquestador) ──
        if database is not None:
            # 1) Verificar en ct_movimientos.atributos (JSON que Flutter guardó)
            row_attrs = await database.fetch_one(
                "SELECT atributos FROM ct_movimientos WHERE id = :id",
                {"id": body.identificador_movimiento},
            )
            attrs = {}
            if row_attrs and row_attrs["atributos"]:
                try:
                    raw = row_attrs["atributos"]
                    attrs = json.loads(raw) if isinstance(raw, str) else (dict(raw) if raw else {})
                except Exception:
                    attrs = {}

            is_app_terpel = attrs.get("isAppTerpel", False)
            datos_factura = attrs.get("DatosFactura", {})
            medio_pago_id = datos_factura.get("medio_pago", 0) if isinstance(datos_factura, dict) else 0
            gopass_v2 = attrs.get("gopass_v2")

            # 2) También verificar payload_fe del body (por si LazoExpress envía datos)
            pfe = dict(body.payload_fe) if body.payload_fe else {}
            medio_pago_body = pfe.get("medio_pago", 0)

            # medio_pago 106=APP TERPEL, 20004=GOPASS
            if is_app_terpel or medio_pago_id in (106, 20004) or gopass_v2 or medio_pago_body in (106, 20004):
                logger.info(
                    "[enviar_fe_pump] ⏸ Medio orquestador detectado (medio_pago=%s, isAppTerpel=%s, gopass_v2=%s, body_medio=%s) "
                    "— NO se envía a 7011. Se esperará callback del orquestador.",
                    medio_pago_id, is_app_terpel, gopass_v2 is not None, medio_pago_body,
                )
                return {
                    "ok": True,
                    "pendiente_orquestador": True,
                    "message": "Pago orquestador: esperando aprobación antes de enviar FE",
                }

        fe_client = await _fe_client_from_url_global()

        # ── Enriquecer payload con pagos ──
        payload = await _enriquecer_payload_con_pagos(
            dict(body.payload_fe), body.identificador_movimiento,
        )

        # ── INSERT transmision antes de enviar (como Java) ──
        id_transmision = await _registrar_transmision(fe_client, payload, body.identificador_movimiento)

        # ── Enviar a 7011 ──
        result = enviar_a_7011_y_opcionalmente_imprimir(
            payload,
            imprimir_despues=body.imprimir_despues,
            identificador_movimiento=body.identificador_movimiento,
            tipo_reporte=body.tipo_reporte or "FACTURA-ELECTRONICA",
            fe_client=fe_client,
        )

        # ── UPDATE transmision con respuesta (como Java) ──
        await _actualizar_transmision(id_transmision, result)

        # Notificar a Flutter vía WebSocket
        if result.get("error_7011"):
            reintento = result.get("reintento_programado", False)
            msg = f"7011 error — la venta se imprimió pero sin CUFE. Mov: {body.identificador_movimiento}"
            if reintento:
                msg += " (reintento automático programado)"
            logger.warning("[enviar_fe_pump] 7011 falló pero se intentó imprimir: %s", result["error_7011"])
            await notification_hub.broadcast({
                "type": "fe_error",
                "title": "Facturador no responde" + (" (reintentando)" if reintento else ""),
                "message": msg,
                "severity": "warning",
                "movimiento_id": body.identificador_movimiento,
                "reintento_programado": reintento,
                "id_transmision": id_transmision,
            })
        else:
            await notification_hub.broadcast({
                "type": "fe_ok",
                "title": "Factura electrónica enviada",
                "message": f"7011 OK — Mov: {body.identificador_movimiento}",
                "severity": "success",
                "movimiento_id": body.identificador_movimiento,
                "id_transmision": id_transmision,
            })

        if result.get("impresion_enviada"):
            await notification_hub.broadcast({
                "type": "print_ok",
                "title": "Ticket impreso",
                "message": f"Impresión enviada para movimiento {body.identificador_movimiento}",
                "severity": "success",
                "movimiento_id": body.identificador_movimiento,
            })

        # Incluir id_transmision en la respuesta
        result["id_transmision"] = id_transmision
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("enviar_fe_pump: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sin-resolver/resolver-y-enviar-fe")
async def resolver_y_enviar_fe(body: ResolverYEnviarFEBody):
    """
    Llamar cuando el usuario en ventas sin resolver asigna cliente y envía.
    Envía a 7011 de inmediato. Si imprimir_despues, dispara impresión (opcional).

    IMPORTANTE: Flutter solo envía datos básicos (documentoCliente, tipoDocumentoCliente, etc.).
    Este endpoint construye el payload completo leyendo ct_movimientos.atributos para
    obtener los datos de venta y cliente que el 7011 (EnviarDatosMovimientoDian) necesita.
    """
    try:
        import json as _json_guard
        fe_client = await _fe_client_from_url_global()
        mov_id = body.identificador_movimiento

        # ── Guard: verificar si el movimiento ya tiene CUFE (ya fue enviado a 7011) ──
        if database is not None:
            try:
                row_cufe = await database.fetch_one(
                    "SELECT atributos FROM ct_movimientos WHERE id = :id",
                    {"id": mov_id},
                )
                if row_cufe and row_cufe["atributos"]:
                    raw_attrs = row_cufe["atributos"]
                    attrs_check = _json_guard.loads(raw_attrs) if isinstance(raw_attrs, str) else (dict(raw_attrs) if raw_attrs else {})
                    fe_data = attrs_check.get("fe_data", {})
                    cufe_existente = fe_data.get("cufe") if isinstance(fe_data, dict) else None
                    if cufe_existente:
                        logger.warning(
                            "[resolver-y-enviar-fe] ⚠ Movimiento %s ya tiene CUFE=%s — NO se reenvía a 7011.",
                            mov_id, cufe_existente[:20] + "..."
                        )
                        return {
                            "ok": True,
                            "ya_enviado": True,
                            "message": f"Movimiento {mov_id} ya fue enviado a 7011 (CUFE existente). No se reenvía.",
                            "cufe": cufe_existente,
                        }
            except Exception as e_guard:
                logger.warning("[resolver-y-enviar-fe] Error verificando CUFE existente: %s", e_guard)

        # ── 1. Leer ct_movimientos.atributos para obtener datos completos ──
        payload = await _construir_payload_desde_movimiento(mov_id, body.payload_fe)
        logger.info("[resolver-y-enviar-fe] Payload construido para mov=%s: keys=%s", mov_id, list(payload.keys()))

        # ── 2. Enriquecer payload con pagos ──
        payload = await _enriquecer_payload_con_pagos(payload, mov_id)

        # ── 3. INSERT transmision ──
        id_transmision = await _registrar_transmision(fe_client, payload, mov_id)

        # ── 4. Enviar a 7011 ──
        result = enviar_venta_sin_resolver_a_7011(
            payload,
            mov_id,
            imprimir_despues=body.imprimir_despues,
            fe_client=fe_client,
        )

        # ── 5. UPDATE transmision ──
        await _actualizar_transmision(id_transmision, result)

        # No lanzar error HTTP si 7011 falla — la venta ya fue gestionada
        result["id_transmision"] = id_transmision
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("resolver_y_enviar_fe: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


async def _construir_payload_desde_movimiento(movimiento_id: int, payload_fe_flutter: dict) -> dict:
    """
    Construye el payload COMPLETO para EnviarDatosMovimientoDian replicando
    exactamente la estructura que StatusPump/Java envía.

    Estructura esperada por 7011:
    {
      "venta": { fecha, consecutivos, bodegas_id, empresas_id, operacion, tercero_*, totales... },
      "detalle": [ { productos_id, producto_descripcion, cantidad, precio, subtotal, impuestos... } ],
      "pagos": [ { medios_pagos_id, valor, recibido, cambio } ],
      "cliente": { ...respuesta completa de consultarCliente con extraData... },
      "retener": false,
      "test": false,
      "identificadorMovimiento": N
    }
    """
    import json as _json
    from datetime import datetime, timezone

    if database is None:
        return payload_fe_flutter

    # ── 1. Leer ct_movimientos con SELECT * (evitar errores de columnas) ──
    try:
        mov = await database.fetch_one(
            "SELECT * FROM ct_movimientos WHERE id = :id",
            {"id": movimiento_id},
        )
    except Exception as e:
        logger.warning("[resolver-y-enviar-fe] Error leyendo ct_movimientos %s: %s", movimiento_id, e)
        mov = None

    if not mov:
        logger.warning("[resolver-y-enviar-fe] Movimiento %s no encontrado, usando payload de Flutter", movimiento_id)
        return payload_fe_flutter

    # Convertir a dict para acceso seguro
    mov = dict(mov)
    logger.info("[resolver-y-enviar-fe] Columnas ct_movimientos: %s", list(mov.keys()))

    def _sg(*keys, default=None):
        """Safe get: intenta múltiples nombres de columna."""
        for k in keys:
            v = mov.get(k)
            if v is not None:
                return v
        return default

    try:
        raw = mov["atributos"]
        if isinstance(raw, str):
            atributos = _json.loads(raw)
        elif isinstance(raw, dict):
            atributos = raw
    except Exception:
        atributos = {}

    # ── 2. Datos del consecutivo desde atributos.consecutivo ──
    cons = atributos.get("consecutivo", {})
    if not isinstance(cons, dict):
        cons = {}

    prefijo = cons.get("prefijo", "SETT")
    consecutivo_actual = cons.get("consecutivo_actual", 0)
    consecutivo_inicial = cons.get("consecutivo_inicial", 1)
    consecutivo_final = cons.get("consecutivo_final", 5000000)
    consecutivo_id = cons.get("id", _sg("consecutivo_id", "ct_consecutivos_id", default=0))

    # ── 3. Datos del cliente: llamar consultarCliente para obtener extraData ──
    doc_cliente = (
        payload_fe_flutter.get("documentoCliente")
        or payload_fe_flutter.get("numeroDocumento")
        or atributos.get("personas_identificacion")
        or "222222222222"
    )
    tipo_doc = (
        payload_fe_flutter.get("tipoDocumentoCliente")
        or payload_fe_flutter.get("tipoDocumento")
        or 13
    )

    # Llamar al servicio consultarCliente para obtener datos completos (con extraData)
    cliente_completo = None
    try:
        if consultar_cliente_externo is not None:
            resp = await consultar_cliente_externo(database, str(doc_cliente), int(tipo_doc))
            if resp.get("success") and resp.get("data"):
                cliente_completo = resp["data"]
                logger.info("[resolver-y-enviar-fe] consultarCliente OK: %s", cliente_completo.get("nombreRazonSocial"))
    except Exception as e:
        logger.warning("[resolver-y-enviar-fe] Error consultarCliente: %s", e)

    # Fallback: construir cliente desde atributos si no hubo respuesta
    if not cliente_completo:
        cliente_db = atributos.get("cliente", {})
        if not isinstance(cliente_db, dict):
            cliente_db = {}
        cliente_completo = {
            **cliente_db,
            "tipoDocumento": int(tipo_doc),
            "numeroDocumento": str(doc_cliente),
            "nombreRazonSocial": (
                payload_fe_flutter.get("nombreRazonSocial")
                or cliente_db.get("nombreRazonSocial")
                or atributos.get("personas_nombre")
                or "CONSUMIDOR FINAL"
            ),
            "nombreComercial": (
                payload_fe_flutter.get("nombreRazonSocial")
                or cliente_db.get("nombreComercial")
                or "CONSUMIDOR FINAL"
            ),
        }

    # Agregar campos que Flutter/Java agregan al cliente
    cliente_completo["identificacion_cliente"] = int(tipo_doc)
    cliente_completo["documentoCliente"] = str(doc_cliente)
    cliente_completo["pendiente_impresion"] = True
    cliente_completo.setdefault("sinSistema", False)
    cliente_completo.setdefault("errorFaltaCampos", False)
    cliente_completo.setdefault("descripcionTipoDocumento", "")

    nombre = cliente_completo.get("nombreRazonSocial") or cliente_completo.get("nombreComercial") or "CONSUMIDOR FINAL"

    # ── 4. Construir objeto "venta" (misma estructura que StatusPump) ──
    fecha_mov = _sg("fecha", default=None)
    if fecha_mov:
        if hasattr(fecha_mov, 'strftime'):
            fecha_str = fecha_mov.strftime("%Y-%m-%d %H:%M:%S")
            fecha_iso = fecha_mov.strftime("%Y-%m-%dT%H:%M:%S-05:00")
        else:
            fecha_str = str(fecha_mov)
            fecha_iso = str(fecha_mov)
    else:
        now = datetime.now()
        fecha_str = now.strftime("%Y-%m-%d %H:%M:%S")
        fecha_iso = now.strftime("%Y-%m-%dT%H:%M:%S-05:00")

    venta_total = float(_sg("venta_total", default=0))
    venta_obj = {
        "fecha": fecha_str,
        "fechaISO": fecha_iso,
        "consecutivo": consecutivo_actual,
        "consecutivoActual": str(consecutivo_actual),
        "consecutivoInicial": str(consecutivo_inicial),
        "consecutivoFinal": str(consecutivo_final),
        "bodegas_id": _sg("bodegas_id", "bodega_id", "ct_bodegas_id", default=1),
        "empresas_id": str(_sg("empresas_id", "empresa_id", "ct_empresas_id", default=0)),
        "dominios_id": 0,
        "operacion": _sg("operacion_id", "operaciones_id", "ct_operaciones_id", "operacion", default=9),
        "movimiento_estado": 12,
        "consecutivo_id": str(consecutivo_id),
        "prefijo": prefijo,
        "persona_id": _sg("personas_id", "persona_id", "ct_personas_id", default=0),
        "persona_nit": " ",
        "persona_nombre": " ",
        "tercero_id": str(doc_cliente),
        "tercero_nit": str(doc_cliente),
        "tercero_tipo_persona": cliente_completo.get("identificadorTipoPersona", 2),
        "tercero_tipo_documento": cliente_completo.get("tipoDocumento", int(tipo_doc)),
        "tercero_nombre": nombre,
        "tercero_responsabilidad_fiscal": cliente_completo.get("tipoResponsabilidad", ""),
        "tercero_codigo_sap": cliente_completo.get("codigoSAP", ""),
        "tercero_correo": cliente_completo.get("correoElectronico", ""),
        "costo_total": float(_sg("costo_total", default=0)),
        "venta_total": venta_total,
        "impuesto_total": float(_sg("impuesto_total", default=0)),
        "descuento_total": float(_sg("descuento_total", default=0)),
        "origen_id": "0",
        "impreso": "N",
        "remoto_id": 0,
        "sincronizado": 0,
        "create_user": 0,
        "create_date": fecha_iso,
        "observaciones": "",
        "consecutivoReferencia": "",
        "prefijoReferencia": "",
        "tipoNegocio": 1,
        "tipoNegocioValor": "X",
    }

    # ── 5. Obtener detalles del movimiento (productos) ──
    detalle = []
    try:
        rows_det = await database.fetch_all(
            """SELECT d.*, p.descripcion as p_descripcion,
                      p.plu as p_plu, p.unidad_medida_id as p_unidad
               FROM ct_movimientos_detalles d
               LEFT JOIN productos p ON p.id = d.productos_id
               WHERE d.movimientos_id = :mid""",
            {"mid": movimiento_id},
        )
        logger.info("[resolver-y-enviar-fe] Detalles encontrados: %d", len(rows_det))
        if rows_det:
            rd0 = dict(rows_det[0])
            logger.info("[resolver-y-enviar-fe] Columnas detalle: %s", list(rd0.keys()))

        for row_d in rows_det:
            rd = dict(row_d)
            prod_id = rd.get("productos_id") or rd.get("producto_id") or 0
            detalle.append({
                "productos_id": prod_id,
                "producto_descripcion": rd.get("p_descripcion") or rd.get("descripcion") or "",
                "productos_plu": str(rd.get("p_plu") or rd.get("plu") or prod_id or ""),
                "cantidad": float(rd.get("cantidad") or 0),
                "costo_unidad": 0,
                "costo_producto": float(rd.get("costo") or rd.get("costo_producto") or 0),
                "precio": float(rd.get("precio") or rd.get("precio_unitario") or 0),
                "descuento_id": 0,
                "descuento_producto": float(rd.get("descuento") or rd.get("descuento_total") or 0),
                "remoto_id": 0,
                "sincronizado": 0,
                "subtotal": float(rd.get("subtotal") or rd.get("sub_total") or 0),
                "base": float(rd.get("precio") or rd.get("precio_unitario") or 0),
                "compuesto": "N",
                "producto_tipo": None,
                "ingredientes": [],
                "impuestos": [],
                "cortesia": False,
                "unidad": rd.get("p_unidad") or rd.get("unidad_medida") or "GLL",
            })
    except Exception as e:
        logger.warning("[resolver-y-enviar-fe] Error leyendo detalles mov=%s: %s", movimiento_id, e)


    # Si no hay detalles, crear uno genérico basado en el total
    if not detalle and venta_total > 0:
        detalle.append({
            "productos_id": 0,
            "producto_descripcion": "VENTA COMBUSTIBLE",
            "productos_plu": "0",
            "cantidad": 1,
            "costo_unidad": 0,
            "costo_producto": 0,
            "precio": venta_total,
            "descuento_id": 0,
            "descuento_producto": 0,
            "remoto_id": 0,
            "sincronizado": 0,
            "subtotal": venta_total,
            "base": venta_total,
            "compuesto": "N",
            "producto_tipo": None,
            "ingredientes": [],
            "impuestos": [],
            "cortesia": False,
            "unidad": "GLL",
        })

    # ── 6. Payload final (misma estructura que StatusPump) ──
    payload_7011 = {
        "venta": venta_obj,
        "detalle": detalle,
        "pagos": [],  # se enriquece después con _enriquecer_payload_con_pagos
        "cliente": cliente_completo,
        "test": False,
        "retener": False,
        "identificadorMovimiento": movimiento_id,
    }

    logger.info(
        "[resolver-y-enviar-fe] Payload armado: mov=%s, doc=%s, tipo=%s, nombre=%s, detalle=%d items, venta_total=%s",
        movimiento_id, doc_cliente, tipo_doc, nombre, len(detalle), venta_total,
    )
    return payload_7011




@router.get("/debug/transmision-exitosa")
async def debug_transmision_exitosa():
    """
    DEBUG: Lee una transmision exitosa (sincronizado=1) de la BD para ver
    la estructura exacta del payload que el 7011 acepta.
    """
    import json as _json
    if database_registry is None:
        raise HTTPException(status_code=500, detail="database_registry no disponible")
    try:
        row = await database_registry.fetch_one(
            """SELECT id, request, response, sincronizado, status
               FROM transmision
               WHERE sincronizado = 1 AND status = 200
               ORDER BY id DESC LIMIT 1"""
        )
        if not row:
            # Buscar cualquiera aunque no exitosa
            row = await database_registry.fetch_one(
                """SELECT id, request, response, sincronizado, status
                   FROM transmision
                   ORDER BY id DESC LIMIT 1"""
            )
        if not row:
            return {"error": "No hay transmisiones en la BD"}

        request_data = row["request"]
        if isinstance(request_data, str):
            request_data = _json.loads(request_data)

        response_data = row["response"]
        if isinstance(response_data, str):
            try:
                response_data = _json.loads(response_data)
            except Exception:
                pass

        return {
            "transmision_id": row["id"],
            "sincronizado": row["sincronizado"],
            "status": row["status"],
            "request_keys": list(request_data.keys()) if isinstance(request_data, dict) else None,
            "request_venta_keys": list(request_data.get("venta", {}).keys()) if isinstance(request_data, dict) else None,
            "request_completo": request_data,
            "response": response_data,
        }
    except Exception as e:
        logger.exception("debug_transmision: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

