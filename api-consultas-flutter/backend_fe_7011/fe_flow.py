"""
Flujo: enviar venta a 7011 (FE) y opcionalmente disparar impresión en LazoExpress.
- Venta pump (Flutter): enviar a 7011 → imprimir factura enviada.
- Venta sin resolver: al resolver (asignar cliente) → enviar a 7011 de inmediato.

Mejoras implementadas:
- Cache de cliente: si el payload ya tiene datos completos del cliente (consultados
  previamente por Flutter), NO vuelve a llamar al 7011. Solo re-consulta si faltan
  campos esenciales (nombreRazonSocial, extraData).
- Resiliencia: si 7011 falla, guarda en cola de reintentos (fe_retry) y programa
  reintentos automáticos cada 2 min.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv

from .fe_7011_client import FE7011Client
from .fe_retry import guardar_pendiente

load_dotenv()  # Cargar .env para PRINT_SERVICE_URL, LAZOEXPRESS_URL, etc.
logger = logging.getLogger(__name__)

# URL de LazoExpress para imprimir (mismo host que el backend o variable de entorno)
LAZOEXPRESS_BASE = os.environ.get("LAZOEXPRESS_URL", "http://127.0.0.1:7010")
PRINT_SERVICE_URL = os.environ.get("PRINT_SERVICE_URL", "http://127.0.0.1:8001")


# ── Campos que consideramos "completos" en un objeto cliente ──
_CAMPOS_CLIENTE_REQUERIDOS = {"nombreRazonSocial", "extraData"}


def _cliente_ya_tiene_datos_completos(cliente: dict) -> bool:
    """
    Retorna True si el objeto cliente ya tiene los datos esenciales
    que devuelve consultarCliente (nombreRazonSocial + extraData).
    Esto significa que Flutter ya consultó al 7011 previamente y
    guardó la respuesta, así que no necesitamos re-consultar.
    """
    if not isinstance(cliente, dict):
        return False
    # Verificar campos esenciales
    tiene_nombre = bool(cliente.get("nombreRazonSocial") or cliente.get("nombreComercial"))
    tiene_extra = bool(cliente.get("extraData"))
    return tiene_nombre and tiene_extra


def _llamar_consultar_cliente_si_falta(client: FE7011Client, payload: dict) -> dict:
    """
    Si el payload tiene cliente con documento/tipo pero le faltan datos de tercero,
    llama a 7011 consultarCliente y actualiza el payload.
    
    OPTIMIZACIÓN: Si el cliente ya tiene datos completos (consultados previamente
    por Flutter vía /ventas/consultar-cliente), NO vuelve a llamar al 7011.
    Solo re-consulta si faltan campos esenciales.
    """
    if "cliente" not in payload or "venta" not in payload:
        return payload
    cliente = payload["cliente"]
    doc = cliente.get("documentoCliente") or (cliente.get("numeroDocumento") and str(cliente["numeroDocumento"]))
    tipo = cliente.get("identificacion_cliente") or cliente.get("tipoDocumento")
    if not doc or tipo is None:
        return payload

    # ── CACHE: si ya tiene datos completos, no re-consultar ──
    if _cliente_ya_tiene_datos_completos(cliente):
        logger.info("[FE-7011] Cliente ya tiene datos completos (cache), saltando consultarCliente")
        # Aún así, actualizar tercero en venta por si falta
        venta = payload["venta"]
        if isinstance(venta, dict):
            venta["tercero_nit"] = cliente.get("numeroDocumento") or cliente.get("documentoCliente") or doc
            venta["tercero_nombre"] = cliente.get("nombreComercial") or cliente.get("nombreRazonSocial") or ""
            venta["tercero_correo"] = cliente.get("correoElectronico") or ""
        return payload

    # ── Sin cache: consultar al 7011 ──
    try:
        logger.info("[FE-7011] Cliente incompleto, consultando al 7011...")
        cliente_consultado = client.consultar_cliente(str(doc), int(tipo))
        if cliente_consultado.get("errorServicio") or cliente_consultado.get("error"):
            return payload
        # Actualizar venta.tercero y reemplazar cliente (simplificado respecto al Java)
        venta = payload["venta"]
        if isinstance(venta, dict):
            venta["tercero_nit"] = cliente_consultado.get("numeroDocumento") or cliente_consultado.get("documentoCliente") or doc
            venta["tercero_nombre"] = cliente_consultado.get("nombreComercial") or cliente_consultado.get("nombreRazonSocial") or ""
            venta["tercero_correo"] = cliente_consultado.get("correoElectronico") or ""
        payload["cliente"] = cliente_consultado
    except Exception as e:
        logger.warning("No se pudo consultar cliente en 7011: %s", e)
    return payload


def enviar_a_7011_y_opcionalmente_imprimir(
    payload_fe: dict[str, Any],
    *,
    imprimir_despues: bool = True,
    identificador_movimiento: Optional[int] = None,
    tipo_reporte: str = "FACTURA-ELECTRONICA",
    fe_client: Optional[FE7011Client] = None,
) -> dict[str, Any]:
    """
    1) Opcionalmente enriquece payload con consulta cliente 7011 (usa cache si hay datos).
    2) Envía a 7011 EnviarDatosMovimientoDian.
    3) Si 7011 falla → guarda en cola de reintentos.
    4) Si imprimir_despues y identificador_movimiento, dispara impresión.

    Retorna algo como: { "ok": True, "response_7011": {...}, "impresion_enviada": True/False }
    """
    client = fe_client or FE7011Client()
    _log = logger.info
    _log("[FE-7011] Inicio flujo: enviar a 7011 + imprimir_despues=%s, movimiento=%s", imprimir_despues, identificador_movimiento)
    payload = _llamar_consultar_cliente_si_falta(client, dict(payload_fe))

    reintento_programado = False
    try:
        _log("[FE-7011] Enviando a EnviarDatosMovimientoDian...")
        response_7011 = client.enviar_datos_movimiento_dian(payload)
        _log("[FE-7011] Respuesta 7011 recibida OK")
        error_7011 = None
    except Exception as e:
        logger.exception("[FE-7011] Error enviando a 7011: %s", e)
        response_7011 = None
        error_7011 = str(e)
        # ── RESILIENCIA: guardar en cola de reintentos ──
        if identificador_movimiento is not None:
            try:
                guardar_pendiente(identificador_movimiento, payload, error_7011)
                reintento_programado = True
                _log("[FE-7011] Guardado en cola de reintentos para mov=%s", identificador_movimiento)
            except Exception as retry_err:
                logger.warning("[FE-7011] No se pudo guardar en cola de reintentos: %s", retry_err)
        # IMPORTANTE: NO retornamos — seguimos para imprimir de todos modos

    impresion_enviada = False
    if imprimir_despues and identificador_movimiento is not None:
        _log("[FE-7011] Disparando impresión movimiento_id=%s tipo=%s (7011_ok=%s)", identificador_movimiento, tipo_reporte, error_7011 is None)
        impresion_enviada = _disparar_impresion(identificador_movimiento, tipo_reporte)
        _log("[FE-7011] Impresión disparada: %s", impresion_enviada)
    else:
        _log("[FE-7011] Sin disparar impresión (imprimir_despues=%s, movimiento=%s)", imprimir_despues, identificador_movimiento)

    return {
        "ok": True,
        "response_7011": response_7011,
        "error_7011": error_7011,
        "impresion_enviada": impresion_enviada,
        "reintento_programado": reintento_programado,
        "payload_enviado": payload,  # Payload enriquecido con datos de cliente
    }


def _disparar_impresion(identificador_movimiento: int, tipo_reporte: str) -> bool:
    """
    Dispara impresión. Soporta dos formatos:
    - Si PRINT_SERVICE_URL está definido: POST .../print-ticket/sales (formato api-consultas / 8001).
    - Si no: POST LAZOEXPRESS_URL/api/imprimir/:reporte (formato LazoExpress 7010).
    """
    import httpx
    use_print_service = os.environ.get("PRINT_SERVICE_URL", "").strip()
    if use_print_service:
        url = f"{use_print_service.rstrip('/')}/print-ticket/sales"
        body = {
            "movement_id": identificador_movimiento,
            "flow_type": "CONSULTAR_VENTAS",
            "report_type": tipo_reporte,
            "body": {},
        }
        logger.info("[FE-7011] POST impresión (print-ticket): %s movement_id=%s", url, identificador_movimiento)
    else:
        url = f"{LAZOEXPRESS_BASE.rstrip('/')}/api/imprimir/{tipo_reporte}"
        body = {"identificadorMovimiento": identificador_movimiento}
        logger.info("[FE-7011] POST impresión (LazoExpress): %s identificadorMovimiento=%s", url, identificador_movimiento)
    try:
        with httpx.Client(timeout=15.0) as client:
            r = client.post(url, json=body)
            if r.status_code == 200:
                logger.info("[FE-7011] Impresión OK movimiento=%s (%s)", identificador_movimiento, tipo_reporte)
                return True
            logger.warning("[FE-7011] Impresión falló %s: %s", r.status_code, r.text[:300])
    except Exception as e:
        logger.warning("[FE-7011] Error disparando impresión: %s", e)
    return False


def enviar_venta_sin_resolver_a_7011(
    payload_fe: dict[str, Any],
    identificador_movimiento: int,
    imprimir_despues: bool = True,
    fe_client: Optional[FE7011Client] = None,
) -> dict[str, Any]:
    """
    Para ventas sin resolver: al asignar cliente y enviar, el backend debe llamar esto
    para enviar a 7011 de inmediato. Si imprimir_despues, dispara impresión (ej. desde historial ya tiene CUFE).
    """
    return enviar_a_7011_y_opcionalmente_imprimir(
        payload_fe,
        imprimir_despues=imprimir_despues,
        identificador_movimiento=identificador_movimiento,
        tipo_reporte="FACTURA-ELECTRONICA",
        fe_client=fe_client,
    )
