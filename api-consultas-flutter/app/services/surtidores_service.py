import json
import os
import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.database import database, database_registry

class SurtidoresService:
    @staticmethod
    async def obtener_mangueras() -> List[Dict[str, Any]]:
        """
        Obtiene el estado actual de todas las mangueras de los surtidores,
        incluyendo si están bloqueadas, su motivo y si tienen saldo de lectura.
        Reemplaza a SetupDao.getCaras()
        """
        query = """
            SELECT 
                id AS configuracion_id,
                surtidor, 
                cara, 
                manguera, 
                motivo_bloqueo, 
                bloqueo,
                salto_lectura
            FROM surtidores_detalles
            ORDER BY surtidor ASC, cara ASC, manguera ASC
        """
        rows = await database.fetch_all(query)
        
        resultados = []
        for row in rows:
            raw_bloqueo = row["bloqueo"]
            is_bloqueado = False
            if raw_bloqueo == 'S' or raw_bloqueo == '1' or raw_bloqueo is True:
                is_bloqueado = True

            raw_salto = row["salto_lectura"]
            tiene_salto = False
            if raw_salto == 'S' or raw_salto == '1' or raw_salto is True:
                tiene_salto = True

            resultados.append({
                "configuracion_id": row["configuracion_id"],
                "surtidor": int(row["surtidor"]) if row["surtidor"] is not None else 0,
                "cara": int(row["cara"]) if row["cara"] is not None else 0,
                "manguera": int(row["manguera"]) if row["manguera"] is not None else 0,
                "bloqueo": is_bloqueado,
                "motivo_bloqueo": row["motivo_bloqueo"] or "",
                "salto_lectura": tiene_salto
            })
            
        return resultados

    @staticmethod
    async def obtener_catalogo_mangueras_precios() -> dict:
        """
        Obtiene el catálogo de todas las caras, mangueras, y el precio del producto asigando.
        Basado en SetupDao.getCaras() del sistema legacy.
        Retorna mapeado para ser usado en Venta Manual (Contingencia).
        """
        query = """
            SELECT 
                sur.cara, 
                sur.manguera, 
                p.id as producto_id,
                p.descripcion as producto_desc, 
                p.precio 
            FROM surtidores_detalles sur
            INNER JOIN productos p ON p.id = sur.productos_id 
            INNER JOIN surtidores s ON sur.surtidores_id = s.id 
            WHERE s.estado = 'A'
            ORDER BY sur.cara ASC, sur.manguera ASC
        """
        try:
            rows = await database.fetch_all(query)
            # Agrupar por Cara para un consumo ágil en Flutter
            caras_dict = {}
            for row in rows:
                cara = str(row["cara"])
                if cara not in caras_dict:
                    caras_dict[cara] = {"mangueras": []}
                
                caras_dict[cara]["mangueras"].append({
                    "manguera": str(row["manguera"]),
                    "producto_id": row["producto_id"],
                    "producto_desc": row["producto_desc"],
                    "precio": float(row["precio"]) if row["precio"] else 0.0
                })
            
            return {"exito": True, "caras": caras_dict}
        except Exception as e:
            print(f"[SURTIDORES] Error en obtener_catalogo_mangueras_precios: {e}")
            return {"exito": False, "caras": {}, "error": str(e)}

    @staticmethod
    async def arreglar_salto_lectura(configuracion_id: int) -> bool:
        """
        Envía comando al Core Gilbarco para corregir el salto de lectura.
        """
        try:
            core_url = os.getenv("GILBARCO_CORE_URL", "http://127.0.0.1:8000/api")
            payload = {
                "tipo": 3,
                "subtipo": 1,
                "paquete": {
                    "configuracionId": configuracion_id
                }
            }
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(core_url, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    # El core responde: {"success":true,"mensaje":"SALTO DE LECTURA CORREGIDO"}
                    if data.get("success"):
                        return True
            return False
        except Exception as e:
            print(f"[SURTIDORES] Error en arreglar salto de lectura: {e}")
            return False

    @staticmethod
    async def actualizar_bloqueos(bloqueos: List[Dict[str, Any]]) -> bool:
        """
        Actualiza el estado de bloqueo de una o múltiples mangueras.
        Reemplaza a SurtidorDao.bloqueosSurtidor(TreeMap<Integer, Boolean> bloqueos)
        
        formato esperado:
        [
            {"manguera": 1, "bloqueo": True, "motivo": "Falla mecánica"},
            {"manguera": 2, "bloqueo": False, "motivo": ""}
        ]
        """
        try:
            for item in bloqueos:
                manguera = item.get("manguera")
                is_bloqueado = item.get("bloqueo", False)
                motivo = item.get("motivo", "")
                
                if manguera is None:
                    continue

                if is_bloqueado:
                    query = "UPDATE surtidores_detalles SET bloqueo = 'S', motivo_bloqueo = :motivo WHERE manguera = :manguera"
                    values = {"motivo": motivo, "manguera": manguera}
                else:
                    query = "UPDATE surtidores_detalles SET bloqueo = NULL, motivo_bloqueo = NULL WHERE manguera = :manguera"
                    values = {"manguera": manguera}
                
                await database.execute(query=query, values=values)

            return True
        except Exception as e:
            print(f"[SURTIDORES] Error en actualizar bloqueos: {e}")
            return False

    @staticmethod
    async def crear_tipo_venta(
        surtidor: int, cara: int, manguera: int,
        tipo_venta: int, promotor_id: Optional[int], monto: int, volumen: int
    ) -> bool:
        """
        Inserta una autorización especial en la tabla transacciones.
        Equivalente a SurtidorDao.crearAutorizacionTipoVenta()
        tipo_venta: 1=Predeterminado, 2=Calibracion, 3=Consumo propio
        """
        try:
            import uuid
            
            # Obtener el grado de la manguera
            query_grado = "SELECT grado FROM surtidores_detalles WHERE manguera = :manguera LIMIT 1"
            row_grado = await database.fetch_one(query=query_grado, values={"manguera": manguera})
            grado = row_grado["grado"] if row_grado else 0
            
            atributos = {
                "tipoVenta": tipo_venta,
                "numeroCara": cara,
                "identificadorPromotor": promotor_id or 0,
                "monto": monto,
                "volumen": volumen
            }
            
            query = """
                INSERT INTO transacciones (
                    codigo, surtidor, cara, grado, proveedores_id, preventa, estado,
                    monto_maximo, cantidad_maxima, fecha_servidor, fecha_creacion, 
                    metodo_pago, medio_autorizacion, trama, transaccion_sincronizada, promotor_id
                ) VALUES (
                    :codigo, :surtidor, :cara, :grado, 1, true, 'A', :monto, :volumen,
                    now(), now(), 1, 'especial', CAST(:trama AS json), 'N', :promotor_id
                )
            """
            
            values = {
                "codigo": str(uuid.uuid4()),
                "surtidor": surtidor,
                "cara": cara,
                "grado": grado,
                "monto": monto,
                "volumen": volumen,
                "trama": json.dumps(atributos),
                "promotor_id": promotor_id
            }
            
            await database.execute(query=query, values=values)
            return True
        except Exception as e:
            print(f"[SURTIDORES] Error en crear_tipo_venta: {e}")
            return False

    @staticmethod
    async def aplicar_multicambioprecio(
        surtidor: int, cara: int, manguera: int, nuevo_precio: int
    ) -> bool:
        """
        Envía un comando de multicambioprecio a LazoExpress port 8000.
        Equivalente a SurtidorMenuPanelController.guardarTipoVenta() para tipoventa=4.
        """
        try:
            core_url = "http://127.0.0.1:8000/api/multicambioprecio"
            
            payload = {
                "identificadorProceso": "flutter-cambio-precio-" + str(datetime.now().timestamp()),
                "listaPrecio": 1,
                "cantdigitos": 6,
                "surtidor": surtidor,
                "data": [
                    {
                        "cara": cara,
                        "precios": [
                            {"manguera": manguera, "precioUnidad": nuevo_precio}
                        ]
                    }
                ]
            }
            
            headers = {
                "Content-Type": "application/json",
                "aplicacion": "terpelpos",
                "identificadordispositivo": "localhost"
            }
            
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(core_url, json=payload, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    # Si devuelve un JSON, asumimos éxito a menos que diga success:false
                    if not data.get("error", False):
                        return True
            return False
        except Exception as e:
            print(f"[SURTIDORES] Error en multicambioprecio: {e}")
            return False

    @staticmethod
    async def obtener_historial_remisiones(registros: int = 50) -> list:
        """
        Consulta de la tabla de SAP el historial de remisiones ingresadas.
        Equivale a infoHistorialRemisiones() en EntradaCombustibleDao.java
        """
        try:
            query = """
                SELECT
                    trs.delivery,
                    p.descripcion as product,
                    trps.quantity,
                    u.alias as unit,
                    to_char(trs.creation_date, 'DD-MM-YYYY') as creation_date,
                    coalesce(to_char(trs.creation_hour, 'HH24:MI:SS'), '') as creation_hour,
                    coalesce(to_char(trs.modification_date, 'DD-MM-YYYY'), '') as modification_date,
                    coalesce(to_char(trs.modification_hour, 'HH24:MI:SS'), '') as modification_hour,
                    tes.descripcion as status
                FROM
                    sap.tbl_remisiones_sap trs
                INNER JOIN sap.tbl_remisiones_productos_sap trps ON
                    trs.id_remision_sap = trps.id_remision_sap
                INNER JOIN productos p ON
                    trps.id_producto = p.id
                INNER JOIN sap.tbl_estados_sap tes ON
                    trps.id_estado = tes.id_estado
                INNER JOIN public.unidades u ON 
                    p.unidad_medida_id = u.id 
                ORDER BY trs.creation_date DESC
                LIMIT :registros
            """
            rows = await database.fetch_all(query=query, values={"registros": registros})
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"[SURTIDORES] Error en obtener_historial_remisiones: {e}")
            return []

    @staticmethod
    async def validar_remision_sap(delivery: str) -> dict:
        """
        Valida que el número de orden de SAP exista y esté disponible.
        """
        try:
            query_remision = """
                SELECT id_remision_sap, delivery, status, logistic_center, supplying_center
                FROM sap.tbl_remisiones_sap
                WHERE delivery = :delivery
            """
            remision = await database.fetch_one(query=query_remision, values={"delivery": delivery})
            if not remision:
                return {"valido": False, "mensaje": "Número de remisión inexistente"}
            
            # Obtener detalles de productos
            query_productos = """
                SELECT trps.id_producto, trps.quantity, p.descripcion
                FROM sap.tbl_remisiones_productos_sap trps
                INNER JOIN public.productos p ON p.id = trps.id_producto 
                WHERE trps.id_remision_sap = :id_remision_sap AND trps.id_estado = 1
            """
            productos = await database.fetch_all(query=query_productos, values={"id_remision_sap": remision["id_remision_sap"]})

            return {
                "valido": True,
                "remision": dict(remision),
                "productos": [dict(p) for p in productos]
            }
        except Exception as e:
            print(f"[SURTIDORES] Error en validar_remision_sap: {e}")
            return {"valido": False, "mensaje": "Fallo al validar en la BD"}

    @staticmethod
    async def obtener_tanques_remision(delivery: str) -> list:
        """
        Ejecuta la funcion SQL que cruza bodegas, inventarios y productos autorizados en SAP.
        """
        try:
            query = "SELECT * FROM public.fnc_consultar_tanques_remision(:delivery);"
            rows = await database.fetch_all(query=query, values={"delivery": delivery})
            
            # Format according to POS expectations
            tanques = []
            for r in rows:
                tanques.append({
                    "id": r["id"],
                    "bodega": r["bodega"],
                    "numero": r["numero"],
                    "volumen_maximo": r["volumen_maximo"],
                    "producto_id": r["producto_id"],
                    "id_remision_producto": r["id_remision_producto"]
                })
            return tanques
        except Exception as e:
            print(f"[SURTIDORES] Error en obtener_tanques_remision: {e}")
            return []

    @staticmethod
    async def obtener_recepciones_pendientes() -> list:
        try:
            # Trae las recepciones incompletas ("PENDIENTES")... asumimos que las pendientes no tienen altura_final
            query = """
                SELECT rc.id, rc.documento, rc.placa, rc.tanques_id, rc.productos_id, 
                       p.descripcion as producto_desc, b.bodega as tanque_desc, rc.fecha, rc.cantidad
                FROM recepcion_combustible rc 
                LEFT JOIN productos p ON rc.productos_id = p.id 
                LEFT JOIN ct_bodegas b ON rc.tanques_id = b.id
                ORDER BY rc.id DESC LIMIT 50;
            """
            rows = await database.fetch_all(query=query)
            res = []
            for r in rows:
                d = dict(r)
                d["estado"] = "PENDIENTE"
                # Parsear fecha a string para flutter
                if d["fecha"]:
                    d["fecha"] = d["fecha"].strftime("%Y-%m-%d %H:%M:%S")
                res.append(d)
            return res
        except Exception as e:
            print(f"[SURTIDORES] Error obteniendo pendientes: {e}")
            return []

    @staticmethod
    async def registrar_recepcion(datos: dict) -> bool:
        """
        Guarda la recepción en la base de datos lazoexpresscore.
        """
        try:
            print(f"[SURTIDORES] --> RECIBIDO PAYLOAD DE RECEPCION: {datos}")
            query = """
                INSERT INTO recepcion_combustible 
                (promotor_id, documento, placa, tanques_id, productos_id, cantidad, fecha, altura_inicial, volumen_inicial, agua_inicial) 
                VALUES 
                (:promotor, :documento, :placa, :tanque_id, :producto_id, :cantidad, CURRENT_TIMESTAMP, :altura_inicial, :volumen_inicial, :agua_inicial)
            """
            
            def safe_float(v):
                try: return float(v)
                except: return 0.0

            val = {
                "promotor": 1, 
                "documento": datos.get("delivery", "MANUAL"),
                "placa": datos.get("placa", "WWW"),
                "tanque_id": datos.get("tanque_id"),
                "producto_id": datos.get("producto_id"),
                "cantidad": safe_float(datos.get("cantidad_reportada")),
                "altura_inicial": safe_float(datos.get("altura_inicial")),
                "volumen_inicial": safe_float(datos.get("volumen_inicial")),
                "agua_inicial": safe_float(datos.get("agua_inicial"))
            }
            await database.execute(query=query, values=val)
            return True
        except Exception as e:
            print(f"[SURTIDORES] Error en registrar_recepcion: {e}")
            return False

    @staticmethod
    async def obtener_tanques_y_productos_globales() -> dict:
        """
        Devuelve el catálogo de Tanques y Productos para uso MANUAL / OFFLINE 
        cuando SAP no tiene el Delivery registrado.
        """
        try:
            # En lazoexpresscore, los tanques de combustible son ct_bodegas con tipo = 'T'
            q_tanques = "SELECT id, cast(atributos::json->>'tanque' as integer) as numero, bodega, coalesce(cast(atributos::json->>'volumenMaximo' as numeric), 99999) as capacidad FROM ct_bodegas WHERE atributos::json->>'tipo' = 'T';"
            # Los productos de combustible están en la tabla productos (sin el prefijo db)
            q_productos = "SELECT id, descripcion FROM productos;"
            
            tanques_raw = await database.fetch_all(query=q_tanques)
            productos_raw = await database.fetch_all(query=q_productos)
            
            print(f"[SURTIDORES] Catalogos cargados: Tanques={len(tanques_raw)} Productos={len(productos_raw)}")
            return {
                "tanques": [dict(t) for t in tanques_raw],
                "productos": [dict(p) for p in productos_raw]
            }
        except Exception as e:
            print(f"[SURTIDORES] Error obteniendo tanques globales: {e}")
            # Fallback tolerante a errores por si las columnas varían
            return {"tanques": [], "productos": []}
