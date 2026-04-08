"""
WebSocket Hub de notificaciones para Flutter.
Mantiene conexiones activas y permite enviar mensajes a todos los clientes.

Uso desde cualquier módulo Python:
    from app.ws_notifications import notification_hub
    await notification_hub.broadcast({
        "type": "fe_error",
        "title": "Error Facturador",
        "message": "7011 no responde",
        "cara": 1,
    })
"""
from __future__ import annotations

import json
import logging
from typing import Any
from fastapi import WebSocket, WebSocketDisconnect, APIRouter

logger = logging.getLogger(__name__)

router = APIRouter()


class NotificationHub:
    """Hub que mantiene clientes WS conectados y hace broadcast."""

    def __init__(self):
        self._clients: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.append(ws)
        logger.info("[WS-Hub] Cliente conectado (%d activos)", len(self._clients))
        # Enviar welcome
        await ws.send_json({"type": "connected", "message": "Conectado al hub de notificaciones"})

    def disconnect(self, ws: WebSocket):
        if ws in self._clients:
            self._clients.remove(ws)
        logger.info("[WS-Hub] Cliente desconectado (%d activos)", len(self._clients))

    async def broadcast(self, data: dict[str, Any]):
        """Enviar mensaje a TODOS los clientes conectados."""
        if not self._clients:
            logger.info("[WS-Hub] No hay clientes conectados, mensaje descartado: %s", data.get("type"))
            return
        
        message = json.dumps(data, ensure_ascii=False)
        dead: list[WebSocket] = []
        for ws in self._clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)
        
        logger.info("[WS-Hub] Broadcast enviado a %d clientes (tipo=%s)", len(self._clients), data.get("type"))


# Instancia global (singleton)
notification_hub = NotificationHub()


@router.websocket("/ws/notifications")
async def ws_notifications(websocket: WebSocket):
    """Endpoint WebSocket para que Flutter se conecte y reciba notificaciones en tiempo real."""
    await notification_hub.connect(websocket)
    try:
        while True:
            # Mantener la conexión viva — leer pings/pongs del cliente
            await websocket.receive_text()
    except WebSocketDisconnect:
        notification_hub.disconnect(websocket)
    except Exception:
        notification_hub.disconnect(websocket)
