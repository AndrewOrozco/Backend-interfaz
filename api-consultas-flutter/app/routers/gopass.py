"""
Router para GoPass - Estado de Pagos
====================================
Permite consultar transacciones GoPass y verificar estado de pagos.

Java: GoPassMenuController.java → GetTransacionesGoPassUseCase, ConsultarEstadoPagoGoPassPort
SQL: fnc_recuperar_ventas_gopass(dias)
Estado check: POST http://localhost:7011/api/consultaEstado
"""
from fastapi import APIRouter, Query
from pydantic import BaseModel
import httpx

from app.database import database

router = APIRouter()

# Cache del POS_ID para no consultarlo cada vez
_pos_id_cache: str | None = None


async def _get_pos_id() -> str:
    """
    Obtiene POS_ID de wacher_parametros.
    Java: Main.parametrosCore.get(NovusConstante.PREFERENCE_POSID)
    """
    global _pos_id_cache
    if _pos_id_cache is not None:
        return _pos_id_cache

    try:
        row = await database.fetch_one(
            "SELECT valor FROM wacher_parametros WHERE codigo = 'POS_ID' LIMIT 1"
        )
        if row:
            _pos_id_cache = str(row["valor"]).strip()
            print(f"[GOPASS] POS_ID obtenido: {_pos_id_cache}")
            return _pos_id_cache
    except Exception as e:
        print(f"[GOPASS] Error obteniendo POS_ID: {e}")

    _pos_id_cache = ""
    return _pos_id_cache


def _extraer_id_movimiento(id_compuesto: int, pos_id: str) -> int:
    """
    Extrae el ID real del movimiento desde el ID compuesto.
    Java: ItemConsultaPago.idMovimiento()
    Quita el POS_ID del inicio y retorna lo que queda.
    """
    if id_compuesto <= 1 or not pos_id:
        return id_compuesto

    id_str = str(id_compuesto)
    if id_str.startswith(pos_id):
        restante = id_str[len(pos_id):]
        if restante:
            return int(restante)
    return id_compuesto


@router.get("/transacciones")
async def obtener_transacciones_gopass(
    dias: int = Query(30, ge=1, le=365, description="Días de historial (default 30)")
):
    """
    Obtener lista de transacciones GoPass.
    Java: GetTransacionesGoPassUseCase → fnc_recuperar_ventas_gopass(dias)
    """
    try:
        pos_id = await _get_pos_id()
        query = "SELECT * FROM fnc_recuperar_ventas_gopass(:dias)"
        rows = await database.fetch_all(query, {"dias": dias})

        transacciones = []
        for idx, row in enumerate(rows):
            r = dict(row)
            keys = list(r.keys())

            # Log primera fila para depurar mapeo de columnas
            if idx == 0:
                print(f"[GOPASS] Columnas SQL ({len(keys)}): {keys}")
                print(f"[GOPASS] Valores fila 0: {list(r.values())}")

            # Mapear por nombre de columna (como hace Gopass.java DAO)
            id_movimiento_compuesto = r.get("idmovimiento", None)

            # Extraer ID real del movimiento (Java: ItemConsultaPago.idMovimiento())
            id_mov_real = None
            if id_movimiento_compuesto is not None:
                id_mov_real = _extraer_id_movimiento(int(id_movimiento_compuesto), pos_id)

            transacciones.append({
                "id_transaccion_gopass": r.get("identificadortransacciongopass", None),
                "isla": r.get("isla", None),
                "codigo_eds": r.get("codigoeds", None),
                "surtidor": r.get("surtidor", None),
                "cara": r.get("cara", None),
                "valor": r.get("valor", None),
                "placa": r.get("placa", None),
                "id_venta_terpel": r.get("identificadorventaterpel", None),
                "estado": str(r.get("estado", "") or ""),
                "fecha": str(r.get("fecha", "") or ""),
                "id_movimiento_compuesto": id_movimiento_compuesto,
                "id_movimiento": id_mov_real,
            })

            if idx == 0:
                print(f"[GOPASS] POS_ID={pos_id}, compuesto={id_movimiento_compuesto}, real={id_mov_real}")

        print(f"[GOPASS] Transacciones encontradas: {len(transacciones)}")
        return {"total": len(transacciones), "transacciones": transacciones}

    except Exception as e:
        print(f"[GOPASS] Error obteniendo transacciones: {e}")
        return {"total": 0, "transacciones": [], "error": str(e)}


class ConsultarEstadoRequest(BaseModel):
    id_transaccion_gopass: int
    id_venta_terpel: int


@router.post("/consultar-estado")
async def consultar_estado_pago(req: ConsultarEstadoRequest):
    """
    Consultar estado de un pago GoPass.
    Java: ConsultarEstadoPagoGoPassUseCase → POST http://localhost:7011/api/consultaEstado
    """
    try:
        # Obtener código de establecimiento
        estacion_row = await database.fetch_one(
            "SELECT codigo_empresa FROM empresas LIMIT 1"
        )
        codigo_estacion = estacion_row["codigo_empresa"] if estacion_row else "EDS1"

        url = "http://localhost:7011/api/consultaEstado"
        body = {
            "refCobro": str(req.id_venta_terpel),
            "establecimiento": codigo_estacion,
            "id": req.id_transaccion_gopass,
        }

        print(f"[GOPASS] Consultando estado: {body}")

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
            )

        print(f"[GOPASS] Estado response: {response.status_code} - {response.text[:300]}")

        if response.status_code == 200:
            data = response.json()
            mensaje = data.get("mensaje", "Estado consultado")
            return {"exito": True, "mensaje": mensaje, "data": data}
        else:
            return {"exito": False, "mensaje": f"Error del servicio: HTTP {response.status_code}"}

    except httpx.ConnectError:
        print("[GOPASS] Servicio GoPass no disponible (puerto 7011)")
        return {"exito": False, "mensaje": "Servicio GoPass no disponible"}
    except Exception as e:
        print(f"[GOPASS] Error consultando estado: {e}")
        return {"exito": False, "mensaje": str(e)}


# ============================================================
# ENVIAR PAGO - Ventas disponibles para GoPass
# ============================================================

@router.get("/ventas-disponibles")
async def obtener_ventas_gopass():
    """
    Ventas elegibles para pago GoPass (últimos 60 min, efectivo, sin GoPass ya asignado).
    Java: Gopass.getVentas() → SQL complejo con filtros.
    
    MEJORA sobre Java: excluir ventas que ya tienen registro en el orquestador
    (transacciones_procesos) para evitar "Ya existe el movimiento" del Go service.
    """
    try:
        query = """
            SELECT c.id, c.fecha, c.venta_total, c.consecutivo, c.prefijo,
                   c.atributos::json->>'cara' AS cara,
                   cmd.cantidad, cmd.precio AS precio_producto,
                   p.descripcion, ttp.id_estado_integracion
            FROM (
                SELECT c.*, row_number() OVER (
                    PARTITION BY c.atributos::json->>'cara' ORDER BY c.fecha DESC
                ) AS rn FROM ct_movimientos c
            ) c
            LEFT JOIN ct_movimientos_detalles cmd ON c.id = cmd.movimientos_id
            LEFT JOIN productos p ON cmd.productos_id = p.id
            LEFT JOIN ct_movimientos_medios_pagos cmmp ON cmmp.ct_movimientos_id = c.id
            LEFT JOIN ct_medios_pagos cmp ON cmp.id = cmmp.ct_medios_pagos_id
            LEFT JOIN (
                SELECT * FROM procesos.tbl_transaccion_proceso ttp
                WHERE ttp.id_integracion = 2
                AND ttp.id_estado_integracion IN (4, 5, 3)
            ) ttp ON ttp.id_movimiento = c.id
            WHERE c.rn <= 6
              AND c.jornadas_id = (SELECT grupo_jornada FROM jornadas LIMIT 1)
              AND (c.atributos::json->>'gopass' IS NULL
                   OR ttp.id_estado_integracion IN (4, 5, 3))
              AND c.fecha BETWEEN (now() - '60 minutes'::interval) AND now()
              AND c.atributos::json->>'cara' IN (
                  SELECT cara::text FROM surtidores_detalles GROUP BY cara
              )
              AND c.tipo = '017'
              AND c.estado_movimiento = '017000'
              AND c.atributos::json->>'isCredito' = 'false'
              AND cmp.id = 1
              -- MEJORA: excluir ventas que ya fueron enviadas al orquestador GoPass
              -- El Go service (5555) rechaza con "Ya existe el movimiento" si se reintenta
              AND NOT EXISTS (
                  SELECT 1 FROM procesos.tbl_transaccion_proceso tp
                  WHERE tp.id_movimiento = c.id
                    AND tp.id_integracion = 1
                    AND tp.id_tipo_integracion = 2
              )
            ORDER BY c.atributos::json->>'cara', c.fecha DESC NULLS LAST
        """
        rows = await database.fetch_all(query)

        ventas = []
        for row in rows:
            r = dict(row)
            ventas.append({
                "id": r.get("id"),
                "fecha": str(r.get("fecha", "")),
                "venta_total": float(r.get("venta_total", 0)),
                "consecutivo": r.get("consecutivo"),
                "prefijo": r.get("prefijo", ""),
                "cara": r.get("cara", ""),
                "cantidad": float(r.get("cantidad", 0)) if r.get("cantidad") else 0,
                "precio_producto": float(r.get("precio_producto", 0)) if r.get("precio_producto") else 0,
                "descripcion": r.get("descripcion", ""),
                "estado_gopass": r.get("id_estado_integracion"),
            })

        print(f"[GOPASS] Ventas disponibles: {len(ventas)}")
        return {"total": len(ventas), "ventas": ventas}

    except Exception as e:
        print(f"[GOPASS] Error obteniendo ventas: {e}")
        return {"total": 0, "ventas": [], "error": str(e)}


# ============================================================
# ENVIAR PAGO - Consultar placas GoPass por venta
# ============================================================

@router.get("/consultar-placas/{venta_id}")
async def consultar_placas_gopass(venta_id: int):
    """
    Consultar placas GoPass disponibles para una venta.
    Java: ConsultarPlacasGoPassPort → GET http://localhost:7011/api/consulta_placas/{ventaId}
    """
    try:
        url = f"http://localhost:7011/api/consulta_placas/{venta_id}"
        print(f"[GOPASS] Consultando placas: {url}")

        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                url,
                headers={"Content-Type": "application/json"},
            )

        print(f"[GOPASS] Placas response: {response.status_code} - {response.text[:300]}")

        if response.status_code == 200:
            data = response.json()
            placas = data.get("datos", [])
            return {"exito": True, "placas": placas}
        else:
            msg = "Sin placas disponibles"
            try:
                err = response.json()
                msg = err.get("mensajeError", msg)
            except Exception:
                pass
            return {"exito": False, "placas": [], "mensaje": msg}

    except httpx.ConnectError:
        return {"exito": False, "placas": [], "mensaje": "Servicio GoPass no disponible (7011)"}
    except Exception as e:
        print(f"[GOPASS] Error consultando placas: {e}")
        return {"exito": False, "placas": [], "mensaje": str(e)}


# ============================================================
# ENVIAR PAGO - Procesar pago GoPass
# ============================================================

class ProcesarPagoRequest(BaseModel):
    venta_id: int
    placa: str
    tag_gopass: str = ""
    nombre_usuario: str = ""


@router.post("/procesar-pago")
async def procesar_pago_gopass(req: ProcesarPagoRequest):
    """
    Procesar pago GoPass.
    Flujo Java: ProcesarPagoGopass →
      1. repository.updateData() → actualizar BD (atributos + medio pago)
      2. EnviandoMedioPago → POST http://localhost:5555/v1/payments/
         Body: { identificadorMovimiento, medioDescription: "GOPASS" }
    """
    try:
        import json as json_lib

        # 1. ANTES del pago: actualizar vehiculo_placa en ct_movimientos.atributos
        # Java: GoPassCarPlateRepository.updateData() →
        #   UPDATE ct_movimientos SET atributos = jsonb_set(atributos::jsonb, '{vehiculo_placa}', to_jsonb(?::varchar)) WHERE id = ?
        # El servicio GoPass (8045) lee vehiculo_placa de la BD para procesar el pago.
        print("[GOPASS] === PASO 1: Actualizando vehiculo_placa en BD ===")
        print(f"[GOPASS] venta_id={req.venta_id}, placa={req.placa}")
        try:
            await _actualizar_movimiento_gopass(
                req.venta_id, "", req.placa, req.nombre_usuario
            )
        except Exception as db_err:
            print(f"[GOPASS] ERROR CRITICO actualizando BD pre-pago: {db_err}")
            import traceback
            traceback.print_exc()
            return {"exito": False, "mensaje": f"Error actualizando placa en BD: {db_err}"}

        # Verificar que la placa se guardó correctamente
        verify = await database.fetch_one(
            "SELECT atributos FROM ct_movimientos WHERE id = :id",
            {"id": req.venta_id},
        )
        if verify:
            attrs_raw = verify["atributos"]
            if isinstance(attrs_raw, str):
                attrs_dict = json_lib.loads(attrs_raw)
            elif isinstance(attrs_raw, dict):
                attrs_dict = attrs_raw
            else:
                attrs_dict = {}
            placa_en_bd = attrs_dict.get("vehiculo_placa", "NO ENCONTRADA")
            print(f"[GOPASS] Verificación: vehiculo_placa en BD = '{placa_en_bd}'")
            if placa_en_bd != req.placa:
                print(f"[GOPASS] ADVERTENCIA: placa en BD '{placa_en_bd}' != placa enviada '{req.placa}'")

        # 2. Enviar pago al orquestador Go (5555)
        # Java: EnviandoMedioPago → POST http://localhost:5555/v1/payments/
        # El Go service rutea GOPASS a 8045/lazo.paymentGoPass/pagoGoPass
        # El Go service retorna 201 (procesamiento async) no 200
        url = "http://localhost:5555/v1/payments/"
        body = {
            "identificadorMovimiento": req.venta_id,
            "medioDescription": "GOPASS",
        }

        print("[GOPASS] === PASO 2: Enviando pago al orquestador Go ===")
        print(f"[GOPASS] POST {url}")
        print(f"[GOPASS] Body: {body}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
            )

        resp_text = response.text[:500] if response.text else ""
        print(f"[GOPASS] Orquestador response: HTTP {response.status_code} - {resp_text}")

        # El Go service retorna 201 (aceptado, procesamiento async)
        # o 200 con resultado directo
        if response.status_code not in (200, 201):
            msg = f"Error del orquestador: HTTP {response.status_code}"
            try:
                err = response.json()
                msg = err.get("mensajeError", err.get("mensaje", msg))
            except Exception:
                pass
            return {"exito": False, "mensaje": msg}

        data = response.json() if resp_text.strip() else {}
        estado_pago = data.get("estadoPago", "").strip()
        mensaje = data.get("mensaje", "").strip()
        id_transaccion = data.get("idTransaccion", "")

        print(f"[GOPASS] estadoPago='{estado_pago}', mensaje='{mensaje}', idTransaccion='{id_transaccion}'")

        # 201 sin estadoPago = el Go service aceptó y procesará async
        # El resultado llegará por WebSocket al frontend (el Go service notifica vía WS)
        if response.status_code == 201 and not estado_pago:
            return {
                "exito": True,
                "estado": "PENDIENTE",
                "mensaje": "Pago enviado al orquestador. El resultado llegará por notificación.",
                "id_transaccion": "",
            }

        if estado_pago == "A" or estado_pago == "APROBADO":
            try:
                await _actualizar_gopass_id(req.venta_id, id_transaccion)
            except Exception as db_err:
                print(f"[GOPASS] Error actualizando gopass ID: {db_err}")

            return {
                "exito": True,
                "estado": "APROBADO",
                "mensaje": mensaje or "Pago enviado a GoPass correctamente",
                "id_transaccion": id_transaccion,
            }
        elif estado_pago == "P" or estado_pago == "PENDIENTE":
            return {
                "exito": True,
                "estado": "PENDIENTE",
                "mensaje": mensaje or "Pago pendiente de confirmación",
                "id_transaccion": id_transaccion,
            }
        else:
            return {
                "exito": False,
                "estado": "RECHAZADO",
                "mensaje": mensaje or "Pago rechazado",
                "id_transaccion": id_transaccion,
            }

    except httpx.ConnectError:
        return {"exito": False, "mensaje": "Orquestador de pagos no disponible (5555)"}
    except Exception as e:
        print(f"[GOPASS] Error procesando pago: {e}")
        import traceback
        traceback.print_exc()
        return {"exito": False, "mensaje": str(e)}


async def _actualizar_movimiento_gopass(
    venta_id: int, gopass_id: str, placa: str, nombre_usuario: str
):
    """
    PRE-PAGO: Solo actualizar vehiculo_placa en atributos.
    Java: GoPassCarPlateRepository.updateData() →
      UPDATE ct_movimientos SET atributos = jsonb_set(atributos::jsonb, '{vehiculo_placa}', to_jsonb(?::varchar))
      WHERE id = ?

    El servicio GoPass (8045) lee vehiculo_placa de ct_movimientos.atributos
    para procesar el pago. Sin esta placa → "idPago o Placa" error.
    """
    import json as json_lib

    # Usar CAST() en vez de :: para evitar conflicto con parser de parámetros de SQLAlchemy
    # SQLAlchemy interpreta ::jsonb como parámetro :jsonb
    placa_jsonb = json_lib.dumps(placa)  # '"JWW176"' → valor jsonb string

    query = """
        UPDATE ct_movimientos
        SET atributos = jsonb_set(
            CAST(COALESCE(atributos, '{}') AS jsonb),
            '{vehiculo_placa}',
            CAST(:placa_val AS jsonb)
        )
        WHERE id = :id
    """
    result = await database.execute(query, {"placa_val": placa_jsonb, "id": venta_id})
    print(f"[GOPASS] vehiculo_placa='{placa}' actualizado para venta {venta_id} (rows affected: {result})")


async def _actualizar_gopass_id(venta_id: int, gopass_id: str):
    """
    POST-PAGO (solo si aprobado): Guardar el ID de transacción GoPass en atributos.
    """
    if not gopass_id:
        return
    import json as json_lib
    gid_jsonb = json_lib.dumps(gopass_id)
    await database.execute(
        "UPDATE ct_movimientos SET atributos = jsonb_set(CAST(COALESCE(atributos, '{}') AS jsonb), '{gopass}', CAST(:gid_val AS jsonb)) WHERE id = :id",
        {"gid_val": gid_jsonb, "id": venta_id},
    )
    print(f"[GOPASS] gopass_id='{gopass_id}' guardado para venta {venta_id}")


class ImprimirGopassRequest(BaseModel):
    movimiento_id: int
    report_type: str = "FACTURA"


@router.post("/imprimir")
async def imprimir_gopass(req: ImprimirGopassRequest):
    """
    Imprimir factura GoPass.
    Flujo: Flutter → Python (8020) → LazoExpress (8010) → Print Service (8001)
    
    LazoExpress endpoint: POST /api/imprimir/FACTURA
    Body: { identificadorMovimiento, body: { datos consumidor final } }
    
    LazoExpress enriquece con venta/detalle/pagos/cliente desde la BD
    y luego reenvía al servicio de impresión en 8001.
    """
    try:
        reporte = req.report_type.upper()
        url = f"http://localhost:8010/api/imprimir/{reporte}"
        body = {
            "identificadorMovimiento": req.movimiento_id,
            "body": {
                "tipoDocumento": "13",
                "numeroDocumento": "2222222",
                "identificadorTipoPersona": 1,
                "nombreComercial": "CLIENTES VARIOS",
                "nombreRazonSocial": "CLIENTES VARIOS",
                "ciudad": "BARRANQUILLA",
                "departamento": "ATLANTICO",
                "regimenFiscal": "48",
                "tipoResponsabilidad": "R-99-PN",
                "codigoSAP": "0",
            },
        }

        print(f"[GOPASS] Imprimiendo venta {req.movimiento_id} → LazoExpress: {url}")
        print(f"[GOPASS] Body: {body}")

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
            )

        print(f"[GOPASS] LazoExpress response: {response.status_code} - {response.text[:300]}")

        if response.status_code >= 200 and response.status_code < 300:
            return {"exito": True, "mensaje": "Venta impresa correctamente"}
        else:
            return {"exito": False, "mensaje": f"Error de LazoExpress: HTTP {response.status_code}"}

    except httpx.ConnectError:
        print("[GOPASS] LazoExpress no disponible (puerto 8010)")
        return {"exito": False, "mensaje": "LazoExpress no disponible (8010)"}
    except Exception as e:
        print(f"[GOPASS] Error imprimiendo: {e}")
        return {"exito": False, "mensaje": str(e)}
