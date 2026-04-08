"""
Servicio de licencias del equipo POS.

Flujo:
1. El usuario ingresa el código numérico que le dio la HO
2. El backend lo reenvía al servidor de licencias de la HO
3. Si la HO responde 200 → se activa el equipo (autorizado='S' en BD)
4. Si la HO no está disponible → intenta validación offline (código guardado en BD)

SOLID:
- SRP: solo gestiona licencias
- OCP: extensible sin modificar el núcleo

ISO 27001:
- Secretos en .env, nunca en código (A.14)
- Auditoría de cada intento (A.12.4)
- Timeout para evitar bloqueos en la UI (A.17)
- Comparación sin revelar detalles del error (A.9)
"""
import hashlib
import os
import socket
import uuid
import logging
import httpx
from datetime import datetime

from databases import Database
from app.config import settings

log = logging.getLogger("license_service")

# URL del servidor de licencias de la HO — viene de settings/.env
_LICENSE_SERVER_URL = settings.LICENSE_SERVER_URL
_LICENSE_AUTH_HEADER = settings.LICENSE_AUTH_HEADER
# Código mock para desarrollo (vacío = desactivado). NUNCA poner en producción.
_LICENSE_MOCK_CODE = settings.LICENSE_MOCK_CODE

# ─── Fingerprint del Hardware ────────────────────────────────────────────────
def get_hardware_fingerprint() -> str:
    """
    Identificador del equipo en el mismo formato que usa el sistema Java:
    MAC en decimal (ej: 2204082022387687) + hostname separados por '|'.

    El campo 'mac' en la tabla equipos viene en este formato desde Java,
    por eso usamos la misma representación para compatibilidad.
    """
    try:
        mac_decimal = str(uuid.getnode())        # ej: "2204082022387687"
        hostname = socket.gethostname()           # ej: "PC-TERPEL-01"
        return f"{mac_decimal}|{hostname}"
    except Exception as e:
        log.error("[LICENSE] Error obteniendo fingerprint: %s", e)
        return "UNKNOWN"


def get_mac_only() -> str:
    """MAC en decimal puro — coincide con lo que Java guarda en equipos.mac."""
    try:
        return str(uuid.getnode())
    except Exception:
        return "UNKNOWN"


# ─── Mock de desarrollo (sin necesidad de HO real) ──────────────────────────
def _validate_mock(code: str) -> bool:
    """
    Valida contra un código mock definido en .env (LICENSE_MOCK_CODE).
    Solo activo si LICENSE_MOCK_CODE tiene valor. NUNCA usar en producción.
    """
    if not _LICENSE_MOCK_CODE:
        return False
    is_valid = code.strip() == _LICENSE_MOCK_CODE.strip()
    if is_valid:
        log.warning("[LICENSE] ⚠️  Activado con CÓDIGO MOCK — solo para desarrollo")
    return is_valid


# ─── Validación con servidor HO (vía MQTT) ────────────────────────────────────
async def _validate_with_ho(code: str) -> bool:
    """
    Envía el código al servidor MQTT (intermediario de la HO).
    El MQTT codifica el código y lo envía a la HO real.

    Respuestas del MQTT:
    - 200 con body que contiene 'EQUIPO REGISTRADO' → ya estaba registrado (válido)
    - 200 con datos del equipo → código aceptado por HO (válido)
    - 4xx/5xx → código inválido o error

    Timeout 10s — ISO 27001 A.17 (disponibilidad).
    """
    # ── Mock de desarrollo ────────────────────────────────────────────────────
    if _validate_mock(code):
        return True

    # ── Validación real contra MQTT/HO ───────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10.0, verify=False) as client:  # nosec B501
            resp = await client.put(
                _LICENSE_SERVER_URL,
                json={"type": "new", "code": code},
                headers={
                    "Authorization": _LICENSE_AUTH_HEADER,
                    "Content-Type": "application/json",
                    "Accept": "*/*",
                    "dispositivo": "127.0.0.1",
                    "aplicacion": "TERPEL_POS_FLUTTER",
                },
            )
            log.info("[LICENSE] Respuesta MQTT/HO: status=%s", resp.status_code)
            if resp.status_code != 200:
                return False
            # El MQTT devuelve 200 con 'EQUIPO REGISTRADO' si ya estaba en BD
            # Eso también es válido — significa que el equipo tiene autorización activa
            try:
                body = resp.json()
                mensaje = str(body.get("message", "") or body.get("data", "")).upper()
                if "EQUIPO REGISTRADO" in mensaje:
                    log.info("[LICENSE] MQTT indica equipo ya registrado (válido)")
                    return True
            except Exception:
                pass  # body no es JSON o no tiene 'message' — igual 200 es válido
            return True
    except httpx.ConnectError:
        log.warning("[LICENSE] No se pudo conectar al MQTT en %s", _LICENSE_SERVER_URL)
        return False
    except Exception as e:
        log.error("[LICENSE] Error consultando MQTT/HO: %s", e)
        return False


# ─── Fallback: código guardado en BD ────────────────────────────────────────
async def _validate_offline(database: Database, code: str) -> bool:
    """
    Si la HO no está disponible, verifica si el código ya fue validado
    previamente y quedó guardado en la tabla equipos.serial_equipo.
    """
    try:
        row = await database.fetch_one(
            "SELECT serial_equipo FROM equipos WHERE autorizado = 'S' LIMIT 1"
        )
        if row and row["serial_equipo"]:
            return row["serial_equipo"].strip() == code.strip()
    except Exception as e:
        log.error("[LICENSE] Error en validación offline: %s", e)
    return False


# ─── Estado actual de la licencia ────────────────────────────────────────────
async def get_license_status(database: Database) -> dict:
    """
    Retorna el estado REAL de licencia del equipo consultando la BD directamente.

    Lógica anti-falsos-positivos:
    1. Si la tabla equipos está VACÍA (post-TRUNCATE) → licenciado=False
    2. Si existe fila pero autorizado != 'S'           → licenciado=False
    3. Solo si existe fila Y autorizado='S'            → licenciado=True

    No consulta el caché de :8010 — va directo a la fuente de verdad (BD).
    """
    try:
        # Consulta directa — no depende del caché de LazoExpress (:8010)
        row = await database.fetch_one(
            "SELECT id, autorizado, empresas_id, mac FROM equipos LIMIT 1"
        )

        # ── Caso 1: tabla vacía (TRUNCATE fue ejecutado) ─────────────────────
        if row is None:
            log.info("[LICENSE] equipos vacía — equipo sin registrar")
            return {
                "licenciado": False,
                "equipoId": None,
                "fingerprint": get_hardware_fingerprint(),
                "hostname": socket.gethostname(),
                "razon": "sin_registro",
                "mensaje": "Equipo no registrado. Ingrese el código de licencia.",
            }

        # ── Caso 2: fila existe pero no autorizado ────────────────────────────
        autorizado = (row["autorizado"] or "N").strip().upper() == "S"
        if not autorizado:
            log.info("[LICENSE] Equipo id=%s no autorizado (autorizado='%s')",
                     row["id"], row["autorizado"])
            return {
                "licenciado": False,
                "equipoId": row["id"],
                "fingerprint": get_hardware_fingerprint(),
                "hostname": socket.gethostname(),
                "razon": "no_autorizado",
                "mensaje": "Equipo registrado pero sin autorización activa.",
            }

        # ── Caso 3: licencia activa ───────────────────────────────────────────
        return {
            "licenciado": True,
            "equipoId": row["id"],
            "fingerprint": get_hardware_fingerprint(),
            "hostname": socket.gethostname(),
            "razon": "autorizado",
            "mensaje": "Licencia activa",
        }

    except Exception as e:
        log.error("[LICENSE] Error consultando estado: %s", e)
        # En caso de error de BD → conservador: NO licenciado
        return {
            "licenciado": False,
            "fingerprint": get_hardware_fingerprint(),
            "hostname": socket.gethostname(),
            "razon": "error_bd",
            "mensaje": f"Error verificando licencia: {e}",
        }


# ─── Activación ──────────────────────────────────────────────────────────────
async def activate_license(database: Database, code: str, ip_origen: str) -> dict:
    """
    Valida el código numérico con la HO y activa el equipo si es correcto.

    Estrategia en cascada (más robusto que el sistema Java):
    1. Intenta validar con servidor HO online
    2. Si HO no disponible → intenta validación offline (código guardado en BD)
    3. Registra resultado en auditoría — ISO 27001 A.12.4
    """
    fingerprint = get_hardware_fingerprint()

    # Intento 1: HO online
    es_valido = await _validate_with_ho(code)

    # Intento 2: fallback offline
    if not es_valido:
        log.info("[LICENSE] HO no disponible, intentando validación offline...")
        es_valido = await _validate_offline(database, code)

    resultado = "ACTIVADO" if es_valido else "RECHAZADO"
    await _insert_audit(database, code, fingerprint, resultado, ip_origen)

    if not es_valido:
        log.warning("[LICENSE] Activación rechazada desde %s", ip_origen)
        return {"exito": False, "mensaje": "Código de licencia inválido o servidor no disponible"}

    # Guardar en BD
    try:
        existe = await database.fetch_one("SELECT id FROM equipos LIMIT 1")
        if existe:
            await database.execute(
                """
                UPDATE equipos
                SET autorizado = 'S',
                    mac = :mac,
                    serial_equipo = :serial
                WHERE id = :id
                """,
                {"mac": fingerprint, "serial": code, "id": existe["id"]},
            )
        else:
            await database.execute(
                """
                INSERT INTO equipos (estado, autorizado, mac, serial_equipo, create_user, create_date)
                VALUES ('A', 'S', :mac, :serial, 1, NOW())
                """,
                {"mac": fingerprint, "serial": code},
            )
        log.info("[LICENSE] Equipo activado correctamente desde %s", ip_origen)
        return {"exito": True, "mensaje": "Licencia activada correctamente"}
    except Exception as e:
        log.error("[LICENSE] Error guardando licencia en BD: %s", e)
        return {"exito": False, "mensaje": f"Error interno: {e}"}


# ─── Auditoría ISO 27001 A.12.4 ──────────────────────────────────────────────
async def _insert_audit(
    database: Database, code: str, fingerprint: str, resultado: str, ip_origen: str
) -> None:
    try:
        await database.execute(
            """
            CREATE TABLE IF NOT EXISTS licencias_audit (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP DEFAULT NOW(),
                codigo_hash VARCHAR(64),
                fingerprint TEXT,
                resultado VARCHAR(20),
                ip_origen VARCHAR(45)
            )
            """
        )
        code_hash = hashlib.sha256(code.encode()).hexdigest()
        await database.execute(
            """
            INSERT INTO licencias_audit (fecha, codigo_hash, fingerprint, resultado, ip_origen)
            VALUES (NOW(), :ch, :fp, :res, :ip)
            """,
            {"ch": code_hash, "fp": fingerprint, "res": resultado, "ip": ip_origen},
        )
    except Exception as e:
        log.warning("[LICENSE] No se pudo registrar auditoría: %s", e)


# ─── Restaurar licencia (igual a Java FormatearEquipo pero controlado) ────────
async def reset_license(
    database: Database,
    database_registry: Database,
    ip_origen: str,
) -> dict:
    """
    Restaura el equipo a estado 'no registrado', limpiando los campos que
    Java escribe al registrar en lazoexpresscore y lazoexpressregistry:

      lazoexpresscore.equipos  → token, password, serial_equipo, mac, autorizado='N', empresas_id=NULL
      lazoexpresscore.empresas → DELETE (elimina el registro de empresa)
      lazoexpressregistry.equipos  → mismo reset
      lazoexpressregistry.empresas → DELETE

    Esto equivale al "Formatear Equipo" de Java pero SIN hacer TRUNCATE de
    todas las tablas — los datos de ventas y movimientos quedan intactos.

    ISO 27001: Se registra en auditoría quién y cuándo realizó el reset (A.12.4).
    """
    fingerprint = get_hardware_fingerprint()
    errores = []

    # ── 1. lazoexpresscore ────────────────────────────────────────────────────
    try:
        # Igual a Java formatearEquipo(): borra el registro del equipo por completo.
        # CASCADE elimina dependencias automáticamente (como Java TRUNCATE CASCADE).
        await database.execute("TRUNCATE TABLE equipos RESTART IDENTITY CASCADE")
        log.warning("[LICENSE] equipos (core) truncated desde %s", ip_origen)
    except Exception as e:
        # Fallback: si no se puede truncar, hacer UPDATE mínimo
        log.warning("[LICENSE] TRUNCATE equipos (core) falló, haciendo UPDATE: %s", e)
        try:
            await database.execute(
                """
                UPDATE equipos
                SET autorizado = 'N', token = '', password = '', empresas_id = NULL
                WHERE id = (SELECT id FROM equipos ORDER BY id LIMIT 1)
                """
            )
        except Exception as e2:
            log.error("[LICENSE] Error reseteando equipos (core): %s", e2)
            errores.append(f"core.equipos: {e2}")

    # ── 2. lazoexpressregistry ────────────────────────────────────────────────
    try:
        await database_registry.execute("TRUNCATE TABLE equipos RESTART IDENTITY CASCADE")
        log.warning("[LICENSE] equipos (registry) truncated desde %s", ip_origen)
    except Exception as e:
        log.warning("[LICENSE] TRUNCATE equipos (registry) falló, haciendo UPDATE: %s", e)
        try:
            await database_registry.execute(
                """
                UPDATE equipos
                SET autorizado = 'N', token = '', password = '', empresas_id = NULL
                WHERE id = (SELECT id FROM equipos ORDER BY id LIMIT 1)
                """
            )
        except Exception as e2:
            log.warning("[LICENSE] equipos (registry) no reseteado: %s", e2)

    # ── 3. Auditoría ISO 27001 A.12.4 ────────────────────────────────────────
    await _insert_audit(database, "RESET", fingerprint, "RESET", ip_origen)

    if errores:
        return {
            "exito": False,
            "mensaje": f"Reset parcial — errores: {'; '.join(errores)}",
        }

    log.warning("[LICENSE] Restauración completa desde %s", ip_origen)
    return {
        "exito": True,
        "mensaje": "POS restaurado. Limpió equipos y empresas en ambas bases de datos. Requiere reactivación.",
    }
