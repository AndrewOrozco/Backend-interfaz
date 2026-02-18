"""
Cola de reintentos para envíos fallidos al 7011 (Facturación Electrónica).

Usa SQLite local para persistir los envíos pendientes.
Un worker en background reintenta cada RETRY_INTERVAL_SECONDS.
Máximo MAX_RETRIES intentos antes de marcar como fallido.

Cuando un reintento es exitoso, notifica vía WebSocket a Flutter.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)
PREFIJ = "[FE-RETRY]"

# ── Configuración ───────────────────────────────────────────
MAX_RETRIES = 5
RETRY_INTERVAL_SECONDS = 120  # 2 minutos
DB_PATH = Path(os.environ.get(
    "FE_RETRY_DB",
    str(Path(__file__).resolve().parent / "fe_pendientes.db"),
))

# ── SQLite setup ────────────────────────────────────────────
_conn: Optional[sqlite3.Connection] = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS fe_pendientes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                movimiento_id INTEGER NOT NULL,
                payload_json  TEXT    NOT NULL,
                intentos      INTEGER DEFAULT 0,
                ultimo_error  TEXT    DEFAULT '',
                estado        TEXT    DEFAULT 'pendiente',  -- pendiente | enviado | fallido
                created_at    TEXT    DEFAULT (datetime('now','localtime')),
                next_retry_at TEXT    DEFAULT (datetime('now','localtime'))
            )
        """)
        _conn.commit()
        logger.info("%s DB inicializada: %s", PREFIJ, DB_PATH)
    return _conn


def guardar_pendiente(movimiento_id: int, payload: dict, error: str = "") -> int:
    """Guarda un envío fallido para reintento posterior. Retorna el id."""
    conn = _get_conn()
    cur = conn.execute(
        """INSERT INTO fe_pendientes (movimiento_id, payload_json, ultimo_error, next_retry_at)
           VALUES (?, ?, ?, ?)""",
        (
            movimiento_id,
            json.dumps(payload, default=str),
            error,
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    row_id = cur.lastrowid
    logger.info("%s Guardado pendiente id=%s mov=%s (error: %s)", PREFIJ, row_id, movimiento_id, error[:120])
    return row_id


def obtener_pendientes() -> list[dict]:
    """Retorna todos los envíos pendientes listos para reintentar."""
    conn = _get_conn()
    rows = conn.execute(
        """SELECT * FROM fe_pendientes
           WHERE estado = 'pendiente'
             AND next_retry_at <= datetime('now','localtime')
           ORDER BY id""",
    ).fetchall()
    return [dict(r) for r in rows]


def marcar_enviado(row_id: int):
    conn = _get_conn()
    conn.execute(
        "UPDATE fe_pendientes SET estado='enviado', intentos=intentos+1 WHERE id=?",
        (row_id,),
    )
    conn.commit()
    logger.info("%s id=%s marcado como ENVIADO ✅", PREFIJ, row_id)


def marcar_fallido(row_id: int, error: str):
    conn = _get_conn()
    conn.execute(
        """UPDATE fe_pendientes
           SET intentos = intentos + 1,
               ultimo_error = ?,
               estado = CASE WHEN intentos + 1 >= ? THEN 'fallido' ELSE 'pendiente' END,
               next_retry_at = datetime('now', 'localtime', '+2 minutes')
           WHERE id = ?""",
        (error, MAX_RETRIES, row_id),
    )
    conn.commit()


def estadisticas() -> dict:
    """Retorna conteo por estado."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT estado, COUNT(*) as cnt FROM fe_pendientes GROUP BY estado"
    ).fetchall()
    return {r["estado"]: r["cnt"] for r in rows}


# ── Worker background ──────────────────────────────────────

async def procesar_pendientes():
    """
    Procesa todos los envíos pendientes.
    Se ejecuta en un loop cada RETRY_INTERVAL_SECONDS desde main.py.
    También actualiza la tabla ``transmision`` en lazoexpressregistry.
    """
    from .fe_7011_client import FE7011Client

    # Obtener host del 7011 si está disponible
    try:
        from app.database import database, database_registry
        from app.url_global import get_host_from_db, ServiciosTerpel
        host = await get_host_from_db(database)
        base_url = ServiciosTerpel.url_base_7011(host)
    except Exception:
        base_url = None
        database_registry = None

    client = FE7011Client(base_url=base_url) if base_url else FE7011Client()
    pendientes = obtener_pendientes()

    if not pendientes:
        return

    logger.info("%s Procesando %d envíos pendientes...", PREFIJ, len(pendientes))

    # Importar notification_hub para WS
    try:
        from app.ws_notifications import notification_hub
    except ImportError:
        notification_hub = None

    # Importar funciones de transmision para actualizar lazoexpressregistry
    try:
        from .fe_transmision import actualizar_transmision_respuesta
    except ImportError:
        actualizar_transmision_respuesta = None

    for p in pendientes:
        row_id = p["id"]
        mov_id = p["movimiento_id"]
        intentos = p["intentos"] + 1
        try:
            payload = json.loads(p["payload_json"])
            logger.info("%s Reintentando id=%s mov=%s (intento %d/%d)", PREFIJ, row_id, mov_id, intentos, MAX_RETRIES)

            response = client.enviar_datos_movimiento_dian(payload)
            marcar_enviado(row_id)

            # ── Actualizar transmision en lazoexpressregistry (sincronizado=1) ──
            if database_registry and actualizar_transmision_respuesta:
                try:
                    # Buscar id_transmision por identificadorMovimiento en request JSON
                    tx_row = await database_registry.fetch_one(
                        "SELECT id FROM transmision WHERE request::json->>'identificadorMovimiento' = :mov_id_str ORDER BY id DESC LIMIT 1",
                        {"mov_id_str": str(mov_id)},
                    )
                    if tx_row:
                        await actualizar_transmision_respuesta(
                            database_registry, int(tx_row["id"]),
                            status_code=200, response_data=response,
                        )
                        logger.info("%s Transmision actualizada (OK) para mov=%s", PREFIJ, mov_id)
                except Exception as tx_err:
                    logger.warning("%s Error actualizando transmision en retry: %s", PREFIJ, tx_err)

            # Notificar Flutter via WS
            if notification_hub:
                await notification_hub.broadcast({
                    "type": "fe_retry_ok",
                    "title": "FE reintento exitoso",
                    "message": f"Factura electrónica enviada (reintento {intentos}). Mov: {mov_id}",
                    "severity": "success",
                    "movimiento_id": mov_id,
                })

        except Exception as e:
            error_msg = str(e)[:300]
            marcar_fallido(row_id, error_msg)
            logger.warning("%s Reintento fallido id=%s mov=%s: %s", PREFIJ, row_id, mov_id, error_msg)

            # ── Actualizar transmision en lazoexpressregistry (error) ──
            if database_registry and actualizar_transmision_respuesta:
                try:
                    tx_row = await database_registry.fetch_one(
                        "SELECT id FROM transmision WHERE request::json->>'identificadorMovimiento' = :mov_id_str ORDER BY id DESC LIMIT 1",
                        {"mov_id_str": str(mov_id)},
                    )
                    if tx_row:
                        await actualizar_transmision_respuesta(
                            database_registry, int(tx_row["id"]),
                            status_code=409, response_data={"error": error_msg},
                        )
                except Exception as tx_err:
                    logger.warning("%s Error actualizando transmision (error) en retry: %s", PREFIJ, tx_err)

            # Si ya se agotaron los reintentos, notificar
            if intentos >= MAX_RETRIES and notification_hub:
                await notification_hub.broadcast({
                    "type": "fe_retry_exhausted",
                    "title": "FE: reintentos agotados",
                    "message": f"No se pudo enviar FE después de {MAX_RETRIES} intentos. Mov: {mov_id}. Requiere atención manual.",
                    "severity": "error",
                    "movimiento_id": mov_id,
                })


async def worker_loop():
    """Loop infinito que procesa pendientes cada RETRY_INTERVAL_SECONDS."""
    logger.info("%s Worker iniciado (cada %ds, max %d reintentos)", PREFIJ, RETRY_INTERVAL_SECONDS, MAX_RETRIES)
    # Inicializar DB
    _get_conn()
    while True:
        try:
            await procesar_pendientes()
        except Exception as e:
            logger.exception("%s Error en worker: %s", PREFIJ, e)
        await asyncio.sleep(RETRY_INTERVAL_SECONDS)
