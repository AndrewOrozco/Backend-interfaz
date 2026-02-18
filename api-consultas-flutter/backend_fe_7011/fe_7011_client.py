"""
Cliente para facturación electrónica (puerto 7011).
Misma lógica que LazoExpress/UI Java: consultarCliente + EnviarDatosMovimientoDian.
Headers compatibles con ClientWSAsync (NovusConstante).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Optional

import httpx

LOG = logging.getLogger(__name__)
PREFIJ = "[FE-7011]"

# Endpoints (sufijos; la base es configurable)
ENDPOINT_CONSULTA_CLIENTE = "/proxi.terpel/consultarCliente"
ENDPOINT_ENVIAR_FE = "/guru.facturacion/EnviarDatosMovimientoDian"

# Formato de fecha que usa el Java (NovusConstante.FORMAT_FULL_DATE_ISO)
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _get_base_url() -> str:
    """URL base del servicio 7011 (ej: https://servicio.terpelpos.com:7011)."""
    host = os.environ.get("FE_7011_HOST", "servicio.terpelpos.com")
    use_ssl = os.environ.get("FE_7011_SSL", "true").lower() == "true"
    scheme = "https" if use_ssl else "http"
    port = os.environ.get("FE_7011_PORT", "7011")
    return f"{scheme}://{host}:{port}"


def build_headers(
    token: Optional[str] = None,
    password: Optional[str] = None,
    identificador_dispositivo: Optional[str] = None,
    aplicacion: Optional[str] = None,
    version_app: Optional[str] = None,
    version_code: Optional[str] = None,
) -> dict[str, str]:
    """
    Headers compatibles con ClientWSAsync (Java).
    Si no se pasan, se leen de env (FE_7011_*) o se usan valores por defecto.
    """
    headers = {
        "Content-Type": "application/json",
        "content-Type": "application/json",
        "fecha": datetime.utcnow().strftime(DATE_FORMAT)[:-3] + "Z",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if password:
        headers["password"] = password
    # Headers DEBEN coincidir con los de url_global.py / Java (get_terpel_headers)
    headers["identificadorDispositivo"] = identificador_dispositivo or os.environ.get("FE_7011_DEVICE_ID", "null-NO TIENE")
    headers["aplicacion"] = aplicacion or os.environ.get("FE_7011_APLICACION", "21.4.1")
    headers["versionApp"] = version_app or os.environ.get("FE_7011_VERSION_APP", "TERPEL")
    headers["versionCode"] = version_code or os.environ.get("FE_7011_VERSION_CODE", "21.4.1")
    return headers


class FE7011Client:
    """Cliente para consultar cliente y enviar datos de movimiento DIAN al 7011."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        password: Optional[str] = None,
        identificador_dispositivo: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = (base_url or _get_base_url()).rstrip("/")
        self.token = token or os.environ.get("FE_7011_TOKEN")
        self.password = password or os.environ.get("FE_7011_PASSWORD")
        self.identificador_dispositivo = identificador_dispositivo or os.environ.get("FE_7011_DEVICE_ID")
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return build_headers(
            token=self.token,
            password=self.password,
            identificador_dispositivo=self.identificador_dispositivo,
        )

    def consultar_cliente(
        self,
        numero_documento: str,
        tipo_documento: int,
    ) -> dict[str, Any]:
        """
        POST .../proxi.terpel/consultarCliente
        Body: { "documentoCliente": "...", "tipoDocumentoCliente": N }
        (mismos nombres que el Java: ConsultaClienteEnviarFE.java)
        """
        url = f"{self.base_url}{ENDPOINT_CONSULTA_CLIENTE}"
        body = {
            "documentoCliente": str(numero_documento),
            "tipoDocumentoCliente": int(tipo_documento),
        }
        LOG.info("%s Consultando cliente: %s", PREFIJ, url)
        LOG.info("%s Body consultarCliente: documento=%s tipo=%s", PREFIJ, numero_documento, tipo_documento)
        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(url, json=body, headers=self._headers())
            r.raise_for_status()
            out = r.json()
            LOG.info("%s Respuesta consultarCliente OK: %s", PREFIJ, out.get("nombreComercial") or out.get("nombreRazonSocial") or "OK")
            return out

    def enviar_datos_movimiento_dian(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        POST .../guru.facturacion/EnviarDatosMovimientoDian
        payload: JSON de venta + cliente (como lo arma la UI Java / FacturacionElectronicaUtils).
        """
        url = f"{self.base_url}{ENDPOINT_ENVIAR_FE}"
        id_mov = payload.get("identificadorMovimiento") or (payload.get("venta") or {}).get("identificadorMovimiento")
        headers = self._headers()
        LOG.info("%s Enviando factura a 7011 (EnviarDatosMovimientoDian): %s", PREFIJ, url)
        LOG.info("%s movimiento_id=%s, headers=%s", PREFIJ, id_mov, headers)
        LOG.info("%s payload keys=%s", PREFIJ, list(payload.keys()))
        import json as _json
        LOG.info("%s payload completo=%s", PREFIJ, _json.dumps(payload, ensure_ascii=False, default=str)[:1000])
        with httpx.Client(timeout=self.timeout, verify=False) as client:
            r = client.post(url, json=payload, headers=headers)
            if r.status_code != 200:
                LOG.error("%s 7011 respondió %s: %s", PREFIJ, r.status_code, r.text[:500])
            r.raise_for_status()
            out = r.json()
            cufe = (out.get("cufe") or out.get("CUFE") or (out.get("data") or {}).get("cufe") or (out.get("data") or {}).get("CUFE")) if isinstance(out, dict) else None
            if cufe:
                LOG.info("%s Factura enviada OK - CUFE: %s", PREFIJ, cufe[: min(32, len(cufe))] + "..." if len(str(cufe)) > 32 else cufe)
            else:
                LOG.info("%s Factura enviada OK - respuesta: %s", PREFIJ, str(out)[:200])
            return out
