"""
Operaciones sobre la tabla ``transmision`` en **lazoexpressregistry**.

Replica la lógica de Java:
  - InsertarTransmisionUseCase  → INSERT con request JSON
  - ActualizarAtributosTransmisionUseCase → UPDATE request enriquecido
  - MovimientosDao.actualizarTransmision   → UPDATE response/status/sincronizado

Tabla (TransmisionEntity.java)::

    id | equipo_id | request (json) | response (text) | url | method
    sincronizado | status | reintentos | reintentos_cliente
    fecha_generado | fecha_ultima | fecha_trasmitido

sincronizado: 0 = no enviado, 1 = éxito, 2 = pendiente
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)
_PREFIJ = "[FE-TRANSMISION]"

# Cache equipo_id para no consultar en cada llamada
_equipo_id_cache: Optional[int] = None


async def obtener_equipo_id(db_registry) -> int:
    """Obtiene equipo_id de la tabla ``equipos`` (equivale a EQUIPO_ID en Java)."""
    global _equipo_id_cache
    if _equipo_id_cache is not None:
        return _equipo_id_cache
    try:
        row = await db_registry.fetch_one("SELECT id FROM equipos LIMIT 1")
        _equipo_id_cache = int(row["id"]) if row else 0
    except Exception as e:
        logger.warning("%s No se pudo obtener equipo_id: %s", _PREFIJ, e)
        _equipo_id_cache = 0
    return _equipo_id_cache


async def insertar_transmision(
    db_registry,
    equipo_id: int,
    request_payload: dict,
    url: str,
    method: str = "POST",
) -> int:
    """
    INSERT en ``transmision`` (lazoexpressregistry).

    Java equivalente: ``fnc_insertar_autorizacion`` /
    ``InsertarTransmisionUseCase``.

    Returns
    -------
    int
        id de la transmisión insertada (0 si falla).
    """
    query = """
        INSERT INTO transmision
            (equipo_id, request, url, method,
             sincronizado, status, reintentos, reintentos_cliente,
             fecha_generado)
        VALUES
            (:equipo_id, CAST(:request AS json), :url, :method,
             0, 0, 0, 0,
             NOW())
        RETURNING id
    """
    request_str = json.dumps(request_payload, ensure_ascii=False, default=str)
    params = {
        "equipo_id": equipo_id,
        "request": request_str,
        "url": url,
        "method": method,
    }
    try:
        row = await db_registry.fetch_one(query, params)
        id_transmision = int(row["id"]) if row else 0
        logger.info(
            "%s Insertada transmision id=%s equipo=%s url=%s",
            _PREFIJ, id_transmision, equipo_id, url,
        )
        return id_transmision
    except Exception as e:
        logger.error("%s Error insertando transmision: %s", _PREFIJ, e)
        return 0


async def actualizar_transmision_respuesta(
    db_registry,
    id_transmision: int,
    status_code: int,
    response_data: Any,
) -> None:
    """
    UPDATE ``transmision`` con la respuesta del servicio 7011.

    Java equivalente: ``MovimientosDao.actualizarTransmision(id, status, response)``.

    - status_code 200 → sincronizado = 1 (éxito)
    - otro → sincronizado = 0 (Java ReenviodeFE puede reintentarlo)
    """
    sincronizado = 1 if status_code == 200 else 0
    response_str = (
        json.dumps(response_data, ensure_ascii=False, default=str)
        if response_data else ""
    )
    query = """
        UPDATE transmision
        SET response      = :response,
            status        = :status,
            sincronizado  = :sincronizado,
            fecha_trasmitido = CASE WHEN :sincronizado_flag = 1
                               THEN NOW() ELSE fecha_trasmitido END,
            fecha_ultima  = NOW()
        WHERE id = :id
    """
    params = {
        "response": response_str,
        "status": status_code,
        "sincronizado": sincronizado,
        "sincronizado_flag": sincronizado,
        "id": id_transmision,
    }
    try:
        await db_registry.execute(query, params)
        logger.info(
            "%s Actualizada transmision id=%s status=%s sincronizado=%s",
            _PREFIJ, id_transmision, status_code, sincronizado,
        )
    except Exception as e:
        logger.error(
            "%s Error actualizando transmision id=%s: %s",
            _PREFIJ, id_transmision, e,
        )


async def actualizar_request_transmision(
    db_registry,
    id_transmision: int,
    request_payload: dict,
) -> None:
    """
    UPDATE ``transmision.request`` con datos enriquecidos (cliente + tercero).

    Java equivalente: ``ActualizarAtributosTransmisionUseCase``
    SQL: ``UPDATE transmision SET request = CAST(? AS json) WHERE id = ?``
    """
    request_str = json.dumps(request_payload, ensure_ascii=False, default=str)
    query = "UPDATE transmision SET request = CAST(:request AS json) WHERE id = :id"
    params = {"request": request_str, "id": id_transmision}
    try:
        await db_registry.execute(query, params)
        logger.info("%s Request actualizado para transmision id=%s", _PREFIJ, id_transmision)
    except Exception as e:
        logger.error("%s Error actualizando request id=%s: %s", _PREFIJ, id_transmision, e)


async def obtener_pagos_movimiento(db_core, movimiento_id: int) -> list[dict]:
    """
    Consulta ``ct_movimientos_medios_pagos`` para obtener los pagos del movimiento.
    Retorna lista de dicts con datos del pago (para incluir en payload FE).
    """
    query = """
        SELECT cmmp.id,
               cmmp.valor_total,
               cmmp.ct_medios_pagos_id,
               cmp.descripcion
        FROM ct_movimientos_medios_pagos cmmp
        INNER JOIN ct_medios_pagos cmp ON cmmp.ct_medios_pagos_id = cmp.id
        WHERE cmmp.ct_movimientos_id = :mov_id
    """
    try:
        rows = await db_core.fetch_all(query, {"mov_id": movimiento_id})
        pagos = []
        for r in rows:
            pagos.append({
                "id": r["id"],
                "valor_total": float(r["valor_total"]) if r["valor_total"] else 0,
                "medioPagoId": r["ct_medios_pagos_id"],
                "descripcion": r["descripcion"] or "",
            })
        if pagos:
            logger.info(
                "%s Pagos movimiento %s: %d medio(s) de pago",
                _PREFIJ, movimiento_id, len(pagos),
            )
        return pagos
    except Exception as e:
        logger.warning("%s Error obteniendo pagos mov=%s: %s", _PREFIJ, movimiento_id, e)
        return []
