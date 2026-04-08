"""
Router para módulo Canastilla (tienda de conveniencia).
Replica el flujo Java: StoreViewController → StoreConfirmarViewController → PedidoFacade.

Constantes Java:
  TIPO_VENTA_CAN = "009"
  ESTADO_MOVIMIENTO_CAN = "009001"
  NEGOCIO_CAN = 3
"""
import json as json_lib
import re
from datetime import datetime

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List

from app.database import database, database_registry

router = APIRouter()

# Java: NovusConstante
TIPO_VENTA_CAN = "009"
ESTADO_MOVIMIENTO_CAN = "009001"
IDENTIFICADOR_CODIGO_BARRA = 2


# ============================================================
# GET /canastilla/productos
# ============================================================
# Java: ObtenerProductosCanastillaUseCase → ProductoCanastillaRepository
# Query: SqlQueryEnum.OBTENER_PRODUCTOS_CANASTILLA

@router.get("/productos")
async def obtener_productos(
    page: int = Query(1, ge=1, description="Página"),
    page_size: int = Query(50, ge=1, le=200, description="Productos por página"),
    buscar: Optional[str] = Query(None, description="Buscar por nombre o PLU"),
    categoria_id: Optional[int] = Query(None, description="Filtrar por categoría ID"),
):
    """
    Obtener productos de canastilla con impuestos, ingredientes, stock.
    Replica: SqlQueryEnum.OBTENER_PRODUCTOS_CANASTILLA
    """
    try:
        offset = (page - 1) * page_size

        where_extra = ""
        params = {"limit_val": page_size, "offset_val": offset}

        if buscar:
            where_extra += " AND (p.descripcion ILIKE :buscar OR p.plu ILIKE :buscar_plu)"
            params["buscar"] = f"%{buscar}%"
            params["buscar_plu"] = f"%{buscar}%"

        if categoria_id and categoria_id > 0:
            where_extra += " AND g.id = :cat_id"
            params["cat_id"] = categoria_id

        query = f"""
            SELECT * FROM (
                SELECT p.id, p.plu, p.estado, p.unidades_medida,
                    p.descripcion, p.precio, p.tipo,
                    p.cantidad_ingredientes, p.cantidad_impuestos,
                    COALESCE(g.id, -1) AS categoria_id,
                    COALESCE(g.grupo, 'OTROS') AS categoria_descripcion,
                    COALESCE((
                        SELECT identificador FROM identificadores i2
                        WHERE entidad_id = p.id AND origen = :origen_cb LIMIT 1
                    ), '') AS codigo_barra,
                    COALESCE((
                        SELECT array_to_json(array_agg(row_to_json(t))) FROM (
                            SELECT i.id AS impuesto_id, i.descripcion, productos_id,
                                   iva_incluido, porcentaje_valor, valor
                            FROM public.productos_impuestos pi
                            INNER JOIN impuestos i ON i.id = pi.impuestos_id
                            WHERE pi.productos_id = p.id
                        ) t
                    ), '[]') AS impuestos,
                    COALESCE((
                        SELECT array_to_json(array_agg(row_to_json(t))) FROM (
                            SELECT pc.*, bp2.costo AS ing_costo, bp2.saldo AS ing_saldo,
                                   pi2.descripcion AS ing_descripcion, pi2.tipo AS ing_tipo
                            FROM productos_compuestos pc
                            INNER JOIN bodegas_productos bp2 ON pc.ingredientes_id = bp2.productos_id
                            INNER JOIN bodegas b2 ON bp2.bodegas_id = b2.id
                            INNER JOIN productos pi2 ON pi2.id = pc.ingredientes_id
                            WHERE pc.productos_id = p.id AND b2.estado != 'I'
                        ) t
                    ), '[]') AS ingredientes,
                    COALESCE(bp.saldo, 0) AS saldo,
                    COALESCE(bp.costo, 0) AS costo,
                    Count(p.id) OVER() AS total_registros
                FROM productos p
                INNER JOIN bodegas_productos bp ON bp.productos_id = p.id
                LEFT JOIN grupos_entidad ge ON p.id = ge.entidad_id
                LEFT JOIN grupos g ON ge.grupo_id = g.id
                WHERE p.estado IN ('A', 'B')
                    AND p.puede_vender = 'S'
                    AND p.id = bp.productos_id
                    AND COALESCE(CAST(p_atributos AS json)->>'tipoStore', 'C') NOT IN ('K', 'T')
                    {where_extra}
                ORDER BY p.descripcion
            ) productos
            WHERE tipo <> -1
            LIMIT :limit_val OFFSET :offset_val
        """

        params["origen_cb"] = IDENTIFICADOR_CODIGO_BARRA
        rows = await database_registry.fetch_all(query, params)

        total = 0
        productos = []
        for row in rows:
            r = dict(row)
            if total == 0:
                total = r.get("total_registros", 0)

            impuestos_raw = r.get("impuestos", "[]")
            if isinstance(impuestos_raw, str):
                impuestos = json_lib.loads(impuestos_raw)
            elif isinstance(impuestos_raw, list):
                impuestos = impuestos_raw
            else:
                impuestos = []

            ingredientes_raw = r.get("ingredientes", "[]")
            if isinstance(ingredientes_raw, str):
                ingredientes = json_lib.loads(ingredientes_raw)
            elif isinstance(ingredientes_raw, list):
                ingredientes = ingredientes_raw
            else:
                ingredientes = []

            tipo = r.get("tipo", 0) or 0
            es_compuesto = tipo in (25, 32)

            productos.append({
                "id": r.get("id"),
                "plu": r.get("plu", ""),
                "descripcion": r.get("descripcion", ""),
                "precio": float(r.get("precio", 0) or 0),
                "tipo": tipo,
                "estado": r.get("estado", ""),
                "unidades_medida": r.get("unidades_medida", ""),
                "saldo": float(r.get("saldo", 0) or 0),
                "costo": float(r.get("costo", 0) or 0),
                "codigo_barra": r.get("codigo_barra", ""),
                "categoria_id": r.get("categoria_id", -1),
                "categoria_descripcion": r.get("categoria_descripcion", "OTROS"),
                "cantidad_impuestos": r.get("cantidad_impuestos", 0),
                "cantidad_ingredientes": r.get("cantidad_ingredientes", 0),
                "es_compuesto": es_compuesto,
                "impuestos": impuestos,
                "ingredientes": ingredientes,
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "productos": productos,
        }

    except Exception as e:
        print(f"[CANASTILLA] Error obteniendo productos: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# GET /canastilla/categorias
# ============================================================
# Java: Uses grupos + grupos_entidad tables

@router.get("/categorias")
async def obtener_categorias():
    """
    Obtener categorías de productos canastilla.
    Java: grupos + grupos_entidad (no productos_familias).
    """
    try:
        query = """
            SELECT
                g.id,
                g.grupo AS descripcion,
                COUNT(DISTINCT ge.entidad_id) AS total_productos
            FROM grupos g
            INNER JOIN grupos_entidad ge ON ge.grupo_id = g.id
            INNER JOIN productos p ON p.id = ge.entidad_id
            INNER JOIN bodegas_productos bp ON bp.productos_id = p.id
            WHERE p.estado IN ('A', 'B')
              AND p.puede_vender = 'S'
              AND COALESCE(CAST(p.p_atributos AS json)->>'tipoStore', 'C') NOT IN ('K', 'T')
            GROUP BY g.id, g.grupo
            ORDER BY g.grupo
        """
        rows = await database_registry.fetch_all(query)

        categorias = []
        for row in rows:
            r = dict(row)
            categorias.append({
                "id": r.get("id"),
                "descripcion": r.get("descripcion", ""),
                "total_productos": r.get("total_productos", 0),
            })

        return {"total": len(categorias), "categorias": categorias}

    except Exception as e:
        print(f"[CANASTILLA] Error obteniendo categorías: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# GET /canastilla/medios-pago
# ============================================================

@router.get("/medios-pago")
async def obtener_medios_pago():
    """
    Obtener medios de pago disponibles para canastilla.
    Java: SetupDao.getMediosPagosDefault(false, "10000, 20004") en lazoexpressregistry
    Luego filtra APP TERPEL en StoreConfirmarViewController.loadMediosPagos().
    
    Excluye: ID 10000, 20004 (GOPASS), APP TERPEL, CON DATAFONO*
    """
    try:
        # Java: getMediosPagosDefault(mostrarDatafono=false, mediosValidar="10000, 20004")
        # FROM medios_pagos WHERE estado='A' AND id NOT IN (10000, 20004)
        #   AND descripcion NOT LIKE 'CON DATAFONO%' ORDER BY id ASC
        query = """
            SELECT id, descripcion, credito, cambio, comprobante, atributos
            FROM medios_pagos
            WHERE estado = 'A'
              AND id NOT IN (10000, 20004)
              AND descripcion NOT LIKE 'CON DATAFONO%'
            ORDER BY id ASC
        """
        rows = await database_registry.fetch_all(query)

        medios = []
        for row in rows:
            r = dict(row)
            desc = r.get("descripcion", "").upper()
            # Java: StoreConfirmarViewController filtra APP TERPEL
            if desc == "APP TERPEL":
                continue
            medios.append({
                "id": r.get("id"),
                "descripcion": r.get("descripcion", ""),
                "credito": r.get("credito", "N") == "S",
                "cambio": r.get("cambio", "N") == "S",
                "comprobante": r.get("comprobante", "N") == "S",
            })

        return {"total": len(medios), "medios_pago": medios}

    except Exception as e:
        print(f"[CANASTILLA] Error obteniendo medios de pago: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# POST /canastilla/procesar-venta
# ============================================================
# Java: PedidoFacade.sendVenta() → MovimientosDao.procesarVentasKiosco()
# Stored procedure: prc_procesar_venta_kiosco_canastilla

class DetalleVentaRequest(BaseModel):
    identificador_producto: int
    nombre_producto: str
    identificacion_producto: str = ""
    cantidad_venta: float
    costo_producto: float = 0
    precio_producto: float
    descuento_total: float = 0
    subtotal_venta: float
    atributos: dict = {}
    impuestos_aplicados: list = []
    ingredientes_aplicados: list = []

class MedioPagoRequest(BaseModel):
    identificacion_medios_pagos: int
    descripcion_medio: str
    recibido_medio_pago: float
    total_medio_pago: float
    vuelto_medio_pago: float = 0
    identificacion_comprobante: str = ""

class ProcesarVentaRequest(BaseModel):
    identificador_promotor: int
    nombres_promotor: str = ""
    apellidos_promotor: str = ""
    identificacion_promotor: str = ""
    identificador_jornada: int
    venta_total: float
    impuesto_total: float
    costo_total: float = 0
    descuento_total: float = 0
    # Java: facturacion_electronica ? '031' : '009' para tipo_documento en consecutivos
    es_facturacion_electronica: bool = False
    factura_electronica: Optional[dict] = None
    detalles: List[DetalleVentaRequest]
    medios_pago: List[MedioPagoRequest]


@router.post("/procesar-venta")
async def procesar_venta(req: ProcesarVentaRequest):
    """
    Procesar venta de canastilla.
    Java: PedidoFacade.sendVenta() → prc_procesar_venta_kiosco_canastilla
    """
    try:
        # Obtener datos de empresa y equipo (registry DB)
        # Nota: según versión de schema, puede ser codigo_empresa o codigo, negocio_id o negocio
        empresa_row = await database_registry.fetch_one(
            "SELECT * FROM empresas e LIMIT 1"
        )
        equipo_row = await database_registry.fetch_one(
            "SELECT id FROM equipos LIMIT 1"
        )
        bodega_row = await database_registry.fetch_one(
            "SELECT * FROM bodegas b WHERE b.estado != 'I' AND b.finalidad = 'C' LIMIT 1"
        )
        # Java: tipo_documento = facturacion_electronica ? '031' : '009'
        # '031' = Factura Electrónica (prefijo ej: SETT), '009' = Factura POS (prefijo ej: POS)
        tipo_doc_consecutivo = "031" if req.es_facturacion_electronica else "009"
        print(f"[CANASTILLA] es_facturacion_electronica={req.es_facturacion_electronica}, tipo_documento={tipo_doc_consecutivo}")

        consecutivo_row = await database_registry.fetch_one(
            """SELECT * FROM consecutivos cs
               WHERE cs.tipo_documento = :tipo_doc
                 AND cs.estado IN ('U', 'A')
                 AND cs.consecutivo_actual < cs.consecutivo_final
               ORDER BY CASE WHEN cs.estado = 'U' THEN 0 ELSE 1 END, cs.id
               LIMIT 1""",
            {"tipo_doc": tipo_doc_consecutivo}
        )

        empresa_id = empresa_row["id"] if empresa_row else 1
        empresa_dict = dict(empresa_row) if empresa_row else {}
        codigo_empresa = empresa_dict.get("codigo_empresa") or empresa_dict.get("codigo") or ""
        nombre_empresa = empresa_dict.get("razon_social") or empresa_dict.get("nombre") or empresa_dict.get("alias") or ""
        negocio_id = empresa_dict.get("negocio_id") or empresa_dict.get("negocio") or 3
        equipo_id = equipo_row["id"] if equipo_row else 1
        bodega_dict = dict(bodega_row) if bodega_row else {}
        bodega_id = bodega_dict.get("id") or 1
        bodega_nombre = bodega_dict.get("nombre") or bodega_dict.get("descripcion") or ""
        bodega_codigo = bodega_dict.get("codigo") or ""
        consecutivo_dict = dict(consecutivo_row) if consecutivo_row else {}
        prefijo = consecutivo_dict.get("prefijo") or ""
        # IMPORTANTE: Java usa consecutivo_actual (el valor actual del consecutivo), NO el id de la fila.
        # El id de la fila es fijo, pero consecutivo_actual se incrementa con cada venta.
        # Si enviamos siempre el mismo valor, el procedimiento dice "VENTA YA EXISTE".
        consecutivo_row_id = consecutivo_dict.get("id") or 0
        ticket_consecutivo = int(consecutivo_dict.get("consecutivo_actual") or 0)
        print(f"[CANASTILLA] Consecutivo: row_id={consecutivo_row_id}, consecutivo_actual={ticket_consecutivo}, prefijo={prefijo}")

        if ticket_consecutivo == 0:
            return {"exito": False, "mensaje": f"No hay consecutivos válidos para canastilla (tipo_documento={tipo_doc_consecutivo})", "movimiento_id": None}

        now = datetime.now()
        fecha_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # Construir JSON de transacción (misma estructura que Java PedidoFacade.sendVenta)
        transaccion = {
            "identificadorNegocio": negocio_id,
            "identificacionEstacion": empresa_id,
            "codigoEstacion": codigo_empresa,
            "prefijo": prefijo,
            "nombreEstacion": nombre_empresa,
            "aliasEstacion": nombre_empresa,
            "identificadorEstacion": str(empresa_id),
            "identificadorTicketVenta": ticket_consecutivo,
            "idTransaccionVenta": ticket_consecutivo,
            "identificadorTicket": ticket_consecutivo,
            "fechaTransaccion": fecha_str,
            "identificadorPromotor": req.identificador_promotor,
            "nombresPromotor": req.nombres_promotor,
            "apellidosPromotor": req.apellidos_promotor,
            "identificacionPromotor": req.identificacion_promotor,
            "identificadorPersona": 3,
            "nombresPersona": "CLIENTES VARIOS",
            "apellidosPersona": "",
            "identificadorProveedor": 0,
            "identificadorBodega": bodega_id,
            "nombresBodega": bodega_nombre,
            "codigoBodega": bodega_codigo,
            "costoTotal": req.costo_total,
            "ventaTotal": req.venta_total,
            "descuentoTotal": req.descuento_total,
            "impuestoTotal": req.impuesto_total,
            "identificadorEquipo": equipo_id,
            "impresoTiquete": "N",
            "usoDolar": 0,
            "identificadorJornada": req.identificador_jornada,
            "identificadorOrigen": 0,
        }

        # Construir detalles de venta
        detalles_venta = []
        for idx, det in enumerate(req.detalles):
            detalle = {
                "idTransaccionVentaDetalle": idx + 1,
                "idTransaccionDetalleVenta": idx + 1,
                "identificadorProducto": det.identificador_producto,
                "nombreProducto": det.nombre_producto,
                "identificacionProducto": det.identificacion_producto,
                "fechaTransaccion": fecha_str,
                "cantidadVenta": det.cantidad_venta,
                "identificadorUnidad": 1,
                "costoProducto": det.costo_producto,
                "precioProducto": det.precio_producto,
                "identificadorDescuento": 0,
                "descuentoTotal": det.descuento_total,
                "subTotalVenta": det.subtotal_venta,
                "atributos": det.atributos,
                "ingredientesAplicados": det.ingredientes_aplicados,
                "impuestosAplicados": det.impuestos_aplicados,
            }
            detalles_venta.append(detalle)

        # Construir medios de pago
        medios_pagos = []
        for mp in req.medios_pago:
            medio = {
                "descripcionMedio": mp.descripcion_medio,
                "identificacionMediosPagos": mp.identificacion_medios_pagos,
                "recibidoMedioPago": mp.recibido_medio_pago,
                "totalMedioPago": mp.total_medio_pago,
                "vueltoMedioPago": mp.vuelto_medio_pago,
                "identificacionComprobante": mp.identificacion_comprobante,
                "monedaLocal": "S",
                "trm": 0,
            }
            medios_pagos.append(medio)

        # JSON completo
        venta_json = {
            "transaccion": transaccion,
            "detallesVenta": detalles_venta,
            "mediosPagos": medios_pagos,
        }

        json_str = json_lib.dumps(venta_json)
        print(f"[CANASTILLA] Procesando venta: {len(req.detalles)} productos, total={req.venta_total}")

        # Llamar al stored procedure con CALL (es procedimiento, no función)
        # Java: call public.prc_procesar_venta_kiosco_canastilla(
        #   i_json_datos => ?::json, i_tipo_transaccion => ?,
        #   i_estado_movimiento => ?, o_json_respuesta => '{}'::json)
        proc_query = """
            CALL public.prc_procesar_venta_kiosco_canastilla(
                i_json_datos => CAST(:json_datos AS json),
                i_tipo_transaccion => :tipo_transaccion,
                i_estado_movimiento => :estado_movimiento,
                o_json_respuesta => CAST('{}' AS json)
            )
        """
        result = await database.fetch_one(proc_query, {
            "json_datos": json_str,
            "tipo_transaccion": TIPO_VENTA_CAN,
            "estado_movimiento": ESTADO_MOVIMIENTO_CAN,
        })

        # Java: rs.getString("o_json_respuesta") → JSON con {codigo, id, ...}
        resp_data = {}
        if result:
            r = dict(result)
            print(f"[CANASTILLA] Raw result keys: {list(r.keys())}")
            print(f"[CANASTILLA] Raw result: {r}")

            # El procedimiento devuelve o_json_respuesta como JSON
            raw_resp = r.get("o_json_respuesta")
            if raw_resp is not None:
                if isinstance(raw_resp, str):
                    try:
                        resp_data = json_lib.loads(raw_resp)
                    except Exception:
                        resp_data = {"raw": raw_resp}
                elif isinstance(raw_resp, dict):
                    resp_data = raw_resp
            else:
                # Fallback: intentar con cualquier columna
                for key, val in r.items():
                    if isinstance(val, str):
                        try:
                            resp_data = json_lib.loads(val)
                            break
                        except Exception:
                            resp_data[key] = val
                    elif isinstance(val, dict):
                        resp_data.update(val)
                    else:
                        resp_data[key] = val

        codigo = resp_data.get("codigo", 0)
        mensaje = resp_data.get("mensaje", "")
        movimiento_id = resp_data.get("id", resp_data.get("movimiento_id", None))
        es_venta_nueva = codigo == 200 and "YA EXISTE" not in mensaje.upper()

        # Si no hay ID directo, buscar el movimiento real en ct_movimientos
        if not movimiento_id and codigo == 200:
            try:
                # Buscar por consecutivo exacto primero (más preciso)
                mov_row = await database.fetch_one(
                    """SELECT id FROM ct_movimientos
                       WHERE consecutivo = :consecutivo
                         AND fecha >= CURRENT_DATE
                       ORDER BY id DESC LIMIT 1""",
                    {"consecutivo": ticket_consecutivo}
                )
                if mov_row:
                    movimiento_id = mov_row["id"]
                    print(f"[CANASTILLA] ID encontrado por consecutivo={ticket_consecutivo}: {movimiento_id}")
                else:
                    # Fallback: buscar por tipo (canastilla = '005' en ct_movimientos)
                    for tipo_buscar in ["005", TIPO_VENTA_CAN]:
                        mov_row = await database.fetch_one(
                            """SELECT id FROM ct_movimientos
                               WHERE tipo = :tipo
                                 AND fecha >= CURRENT_DATE
                               ORDER BY id DESC LIMIT 1""",
                            {"tipo": tipo_buscar}
                        )
                        if mov_row:
                            movimiento_id = mov_row["id"]
                            print(f"[CANASTILLA] ID encontrado con tipo={tipo_buscar}: {movimiento_id}")
                            break
            except Exception as e2:
                print(f"[CANASTILLA] Error buscando ID en ct_movimientos: {e2}")
                try:
                    mov_row2 = await database.fetch_one(
                        """SELECT id FROM ct_movimientos
                           WHERE fecha >= CURRENT_DATE
                           ORDER BY id DESC LIMIT 1"""
                    )
                    if mov_row2:
                        movimiento_id = mov_row2["id"]
                        print(f"[CANASTILLA] ID encontrado (último del día): {movimiento_id}")
                except Exception as e3:
                    print(f"[CANASTILLA] Error fallback buscando ID: {e3}")

        # Último fallback: extraer del mensaje (ej: "POS - 1638")
        # NOTA: este número es el consecutivo, NO el ct_movimientos.id
        if not movimiento_id:
            match = re.search(r'POS\s*-\s*(\d+)', mensaje)
            if match:
                consecutivo_del_msg = int(match.group(1))
                # Buscar el ct_movimientos.id real por este consecutivo
                try:
                    mov_row = await database.fetch_one(
                        """SELECT id FROM ct_movimientos
                           WHERE consecutivo = :consecutivo
                           ORDER BY id DESC LIMIT 1""",
                        {"consecutivo": consecutivo_del_msg}
                    )
                    if mov_row:
                        movimiento_id = mov_row["id"]
                        print(f"[CANASTILLA] ID encontrado por consecutivo del mensaje ({consecutivo_del_msg}): {movimiento_id}")
                    else:
                        print(f"[CANASTILLA] ADVERTENCIA: consecutivo {consecutivo_del_msg} del mensaje no encontrado en ct_movimientos")
                except Exception:
                    pass

        print(f"[CANASTILLA] Resultado: codigo={codigo}, id={movimiento_id}, nueva={es_venta_nueva}")

        # Java: después de venta exitosa, incrementar consecutivo_actual + 1
        # Query de Java: UPDATE consecutivos SET consecutivo_actual = consecutivo_actual + 1 WHERE id = $1
        if es_venta_nueva and consecutivo_row_id:
            try:
                await database_registry.execute(
                    """UPDATE consecutivos
                       SET consecutivo_actual = consecutivo_actual + 1
                       WHERE id = :row_id""",
                    {"row_id": consecutivo_row_id}
                )
                print(f"[CANASTILLA] Consecutivo incrementado: row_id={consecutivo_row_id}, anterior={ticket_consecutivo}")
            except Exception as e_cons:
                print(f"[CANASTILLA] Error incrementando consecutivo: {e_cons}")

        if codigo == 200 or movimiento_id:
            return {
                "exito": True,
                "mensaje": resp_data.get("mensaje", "Venta procesada exitosamente"),
                "movimiento_id": movimiento_id,
                "data": resp_data,
            }
        else:
            print(f"[CANASTILLA] Venta con error, respuesta: {resp_data}")
            return {
                "exito": False,
                "mensaje": resp_data.get("mensaje", "Error procesando venta"),
                "movimiento_id": movimiento_id,
                "data": resp_data,
            }

    except Exception as e:
        print(f"[CANASTILLA] Error procesando venta: {e}")
        import traceback
        traceback.print_exc()
        return {"exito": False, "mensaje": str(e), "movimiento_id": None}


# ============================================================
# GET /canastilla/historial
# ============================================================
# Java: VentasCanastillaRepository → fnc_consultar_ventas_canastilla

@router.get("/historial")
async def obtener_historial(
    fecha_inicio: str = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    fecha_fin: str = Query(..., description="Fecha fin (YYYY-MM-DD)"),
    promotor: Optional[str] = Query(None, description="Filtrar por promotor"),
):
    """
    Historial de ventas canastilla.
    Java: VentasCanastillaRepository.findVentasCanastilla() → fnc_consultar_ventas_canastilla
    """
    try:
        prom = promotor if promotor else ""
        query = "SELECT * FROM fnc_consultar_ventas_canastilla(:fi, :ff, :prom) resultado"
        rows = await database.fetch_all(query, {"fi": fecha_inicio, "ff": fecha_fin, "prom": prom})

        ventas = []
        for row in rows:
            r = dict(row)
            resultado = r.get("resultado", None)
            if resultado:
                if isinstance(resultado, str):
                    try:
                        data = json_lib.loads(resultado)
                        if isinstance(data, list):
                            ventas.extend(data)
                        else:
                            ventas.append(data)
                    except Exception:
                        ventas.append({"raw": resultado})
                elif isinstance(resultado, dict):
                    ventas.append(resultado)
                elif isinstance(resultado, list):
                    ventas.extend(resultado)
            else:
                ventas.append(r)

        return {"total": len(ventas), "ventas": ventas}

    except Exception as e:
        print(f"[CANASTILLA] Error obteniendo historial: {e}")
        import traceback
        traceback.print_exc()
        return {"total": 0, "ventas": [], "error": str(e)}


# ============================================================
# POST /canastilla/imprimir
# ============================================================
# Java: PrinterFacade.printRecibo() → print service 8001

class ImprimirCanastillaRequest(BaseModel):
    movimiento_id: int
    report_type: str = "VENTA"
    cliente: Optional[dict] = None

@router.post("/imprimir")
async def imprimir_canastilla(req: ImprimirCanastillaRequest):
    """
    Imprimir recibo de canastilla.
    Construye un body rico (resoluciones, cliente, venta) igual que Java,
    para que el print service tenga todos los datos del ticket.
    """
    try:
        from decimal import Decimal
        def _num(v):
            """Convierte Decimal/otros a float para JSON serialización."""
            if isinstance(v, Decimal):
                return float(v)
            if v is None:
                return 0
            return v

        report_type = req.report_type.upper()
        url = f"http://localhost:8001/api/imprimir/{report_type}"

        # ── 1. Consultar datos del movimiento ──
        mov = await database.fetch_one(
            """SELECT id, consecutivo, fecha, venta_total, impuesto_total,
                      descuento_total, costo_total, tipo, estado_movimiento,
                      atributos, responsables_id, personas_id, equipos_id
               FROM ct_movimientos WHERE id = :mid""",
            {"mid": req.movimiento_id}
        )
        mov_dict = dict(mov) if mov else {}
        consecutivo_valor = _num(mov_dict.get("consecutivo", 0))

        # ── 2. Consultar resolución / consecutivo (registry) ──
        # Si report_type es FACTURA-ELECTRONICA → buscar consecutivo '031' (FE)
        # Si no → buscar '009' (POS)
        tipo_doc_print = "031" if report_type == "FACTURA-ELECTRONICA" else "009"
        cons_row = await database_registry.fetch_one(
            """SELECT id, prefijo, resolucion, consecutivo_inicial,
                      consecutivo_final, consecutivo_actual,
                      fecha_inicio, fecha_fin, observaciones
               FROM consecutivos
               WHERE tipo_documento = :tipo_doc AND estado IN ('U', 'A')
               ORDER BY CASE WHEN estado = 'U' THEN 0 ELSE 1 END
               LIMIT 1""",
            {"tipo_doc": tipo_doc_print}
        )
        cons = dict(cons_row) if cons_row else {}
        prefijo = cons.get("prefijo") or ""

        # ── 3. Consultar empresa (registry) ──
        emp_row = await database_registry.fetch_one("SELECT * FROM empresas LIMIT 1")
        emp = dict(emp_row) if emp_row else {}
        emp_nit = emp.get("nit") or emp.get("documento") or ""
        emp_nombre = emp.get("razon_social") or emp.get("nombre") or emp.get("alias") or ""
        emp_direccion = emp.get("direccion") or ""
        emp_telefono = emp.get("telefono") or ""
        emp_ciudad = emp.get("ciudad") or emp.get("ciudades_descripcion") or ""

        # ── 4. Datos del cliente (actualizados para ticket y eventual 7011) ──
        cliente_data = req.cliente or {}
        tercero_nombre = (cliente_data.get("nombreComercial") or cliente_data.get("nombreRazonSocial") or "").strip() or "CONSUMIDOR FINAL"
        # Cédula/identificación siempre como string para ticket y FE
        _doc = cliente_data.get("numeroDocumento") or cliente_data.get("identificacion") or cliente_data.get("documentoCliente") or ""
        tercero_nit = str(_doc).strip() if _doc not in (None, "") else ""
        tercero_tipo_doc = cliente_data.get("tipoDocumento") or cliente_data.get("identificacion_cliente") or 1
        tercero_correo = (cliente_data.get("correoElectronico") or "").strip()
        tercero_direccion = (cliente_data.get("direccionTicket") or cliente_data.get("direccion") or "").strip()
        tercero_telefono = (cliente_data.get("telefonoTicket") or cliente_data.get("telefono") or "").strip()

        # ── 5. R. Interna: buscar último registro de r_interna para esta resolución ──
        r_interna = ""
        try:
            ri_row = await database.fetch_one(
                """SELECT atributos::text FROM ct_movimientos
                   WHERE id = :mid""",
                {"mid": req.movimiento_id}
            )
            if ri_row:
                import json as _json
                try:
                    attrs = _json.loads(ri_row[0]) if isinstance(ri_row[0], str) else (ri_row[0] or {})
                    r_interna = attrs.get("r_interna", "") or attrs.get("rinterna", "") or ""
                except Exception:
                    pass
        except Exception:
            pass

        # ── 6. Construir body rico (igual que Java envía a /api/imprimir) ──
        venta_data = {
            "consecutivo": consecutivo_valor,
            "prefijo": prefijo,
            "r_interna": r_interna,
            "tercero_nombre": tercero_nombre,
            "tercero_nit": tercero_nit,
            "tercero_tipo_documento": tercero_tipo_doc,
            "tercero_tipo_persona": 1,
            "tercero_responsabilidad_fiscal": "",
            "tercero_correo": tercero_correo,
            "tipo_negocio": 3,  # CANASTILLA
            "total_bruto": _num(mov_dict.get("venta_total", 0)),
            "total_base_imponible": _num(mov_dict.get("venta_total", 0)),
            "impuesto_total": _num(mov_dict.get("impuesto_total", 0)),
        }

        resoluciones_data = {
            "prefijo": prefijo,
            "resolucion": cons.get("resolucion") or "",
            "consecutivo_inicial": _num(cons.get("consecutivo_inicial") or 0),
            "consecutivo_final": _num(cons.get("consecutivo_final") or 0),
            "fecha_inicio": str(cons.get("fecha_inicio") or "")[:10],
            "fecha_fin": str(cons.get("fecha_fin") or "")[:10],
            "observaciones": cons.get("observaciones") or "",
        }

        tipo_empresa = {
            "nit": emp_nit,
            "razon_social": emp_nombre,
            "direccion": emp_direccion,
            "telefono": emp_telefono,
            "ciudad": emp_ciudad,
        }

        # Body listo para impresión (8001) y reutilizable para envío FE a 7011 si se implementa
        payload = {
            "identificadorMovimiento": req.movimiento_id,
            "body": {
                "resoluciones": resoluciones_data,
                "tipoEmpresa": tipo_empresa,
                "cliente": {
                    "nombre": tercero_nombre,
                    "identificacion": tercero_nit,
                    "numeroDocumento": tercero_nit,
                    "tipoDocumento": tercero_tipo_doc,
                    "correo": tercero_correo,
                    "direccion": tercero_direccion,
                    "telefono": tercero_telefono,
                },
                "venta": venta_data,
            },
        }

        print(f"[CANASTILLA] Imprimiendo venta {req.movimiento_id} → {url}")
        print(f"[CANASTILLA] report_type={report_type}, consecutivo={consecutivo_valor}, prefijo={prefijo}")

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        print(f"[CANASTILLA] Print response: {response.status_code} - {response.text[:300]}")

        if 200 <= response.status_code < 300:
            return {"exito": True, "mensaje": "Impresión enviada"}
        else:
            print(f"[CANASTILLA] /api/imprimir falló ({response.status_code}), intentando fallback...")
            return await _imprimir_via_sales_flow(req)

    except httpx.ConnectError:
        return {"exito": False, "mensaje": "Servicio de impresión no disponible (8001)"}
    except Exception as e:
        print(f"[CANASTILLA] Error imprimiendo: {e}")
        import traceback
        traceback.print_exc()
        return {"exito": False, "mensaje": str(e)}


async def _imprimir_via_sales_flow(req: ImprimirCanastillaRequest):
    """Fallback: imprimir via /print-ticket/sales con body poblado para evitar validación."""
    try:
        url = "http://localhost:8001/print-ticket/sales"

        # Enviar body NO vacío para que el controller salte la validación de movimiento
        # y cree el SaleDocument directamente desde el body
        body_data = {
            "identificadorMovimiento": req.movimiento_id,
            "tipo_negocio": "CANASTILLA",
            "database_source": "REGISTRO",
        }
        if req.cliente:
            body_data["cliente"] = req.cliente

        payload = {
            "movement_id": req.movimiento_id,
            "flow_type": "CONSULTAR_VENTAS",
            "report_type": req.report_type.upper(),
            "database_source": "REGISTRO",
            "tipo_negocio": "CANASTILLA",
            "body": body_data,
        }

        print(f"[CANASTILLA] Fallback /print-ticket/sales → {url}")
        print(f"[CANASTILLA] Payload: {payload}")

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

        print(f"[CANASTILLA] Fallback response: {response.status_code} - {response.text[:300]}")

        if 200 <= response.status_code < 300:
            return {"exito": True, "mensaje": "Impresión enviada"}
        else:
            return {"exito": False, "mensaje": f"Error impresión: HTTP {response.status_code}"}

    except Exception as e:
        print(f"[CANASTILLA] Error en fallback: {e}")
        return {"exito": False, "mensaje": str(e)}


# ============================================================
# GET /canastilla/config-facturacion
# ============================================================
# Java: Main.getParametroCoreBoolean("FACTURACION", false)
# → wacher_parametros WHERE codigo = 'FACTURACION' → valor 'S'/'N'

@router.get("/config-facturacion")
async def obtener_config_facturacion():
    """
    Consulta configuración de facturación POS y FE.
    Java: Main.getParametroCoreBoolean("FACTURACION", false)
    Java: FacturacionElectronica.isDefaultFe() → wacher_parametros codigo='DEFAULT_FE' valor='S'
    Si isDefaultFe = true → botón cambia de 'F. ELECTRONICA' a 'FACTURA POS'
    """
    try:
        # 1) facturacionPOS: wacher_parametros WHERE codigo = 'FACTURACION'
        row_fac = await database.fetch_one(
            "SELECT valor FROM wacher_parametros WHERE codigo = 'FACTURACION' LIMIT 1"
        )
        facturacion_pos = False
        if row_fac and row_fac['valor']:
            facturacion_pos = str(row_fac['valor']).strip().upper() == 'S'

        # 2) isDefaultFe: Java → FacturacionElectronica.isDefaultFe()
        #    SELECT valor FROM wacher_parametros WHERE codigo = 'DEFAULT_FE'
        #    Si valor = 'S' → true (botón "FACTURA POS"), sino false (botón "F. ELECTRONICA")
        row_fe = await database.fetch_one(
            "SELECT valor FROM wacher_parametros WHERE codigo = 'DEFAULT_FE' LIMIT 1"
        )
        is_default_fe = False
        if row_fe and row_fe['valor']:
            is_default_fe = str(row_fe['valor']).strip().upper() == 'S'

        print(f"[CANASTILLA] Config: facturacionPOS={facturacion_pos}, isDefaultFe={is_default_fe}")
        return {
            "facturacion_pos": facturacion_pos,
            "is_default_fe": is_default_fe,
        }

    except Exception as e:
        print(f"[CANASTILLA] Error consultando config facturación: {e}")
        return {"facturacion_pos": False, "is_default_fe": False}
