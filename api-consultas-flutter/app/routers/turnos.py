"""
Router para Gestión de Turnos (Jornadas)
=========================================
Orquesta el inicio y cierre de turnos de promotores:
- Puerto 8019: Lectura de totalizadores de surtidores (aperturaTurno)
- Puerto 8010: Registro de jornadas (api/jornada/iniciar, api/jornada/finalizar)
- Base de datos: jornadas, personas, equipos, surtidores_core

Flujo inicio de turno:
1. Flutter obtiene surtidores disponibles
2. Python lee totalizadores de cada surtidor vía 8019
3. Core Gilbarco envía lectura RFID al backend (reutiliza /rumbo/api/identificadorPromotor)
4. Flutter valida promotor y envía saldo
5. Python registra jornada en LazoExpress 8010
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import json
import httpx
from datetime import datetime

from app.database import database
from app.config import settings

router = APIRouter()

LAZO_HOST = getattr(settings, 'LAZO_HOST', 'localhost')

# Headers que usa Java para llamar al servicio de totalizadores (8019)
TOTALIZADOR_HEADERS = {
    "content-Type": "application/json",
    "authorization": "123344",
    "fecha": "2020-09-09T11:46:35-05:00",
    "aplicacion": "LAZO_EXPRESS_MANAGER",
    "identificadorDispositivo": "localhost",
    "uuid": "ef115004-39bd-4c51-945c-5ce4eb0c23c9",
    "password": "12345",
    "versioncode": "1",
    "versionapp": "1.0.0",
}


# ============================================================
# SCHEMAS
# ============================================================

class TotalizadorRequest(BaseModel):
    surtidor: int
    host: str

class ValidarPromotorRequest(BaseModel):
    identificacion: str
    pin: Optional[str] = None

class IniciarTurnoRequest(BaseModel):
    personas_id: int
    saldo: int = 0
    surtidores: List[int] = []
    totalizadores: Optional[list] = None
    es_principal: bool = True


class PersonaCierreRequest(BaseModel):
    personas_id: int
    identificadorJornada: Optional[int] = None
    grupo_jornada: Optional[int] = None


class FinalizarTurnoRequest(BaseModel):
    personas: List[PersonaCierreRequest]
    totalizadoresFinales: Optional[list] = None
    es_principal: bool = False


# ============================================================
# ENDPOINTS
# ============================================================

@router.get("/surtidores-estacion")
async def obtener_surtidores_estacion():
    """
    Obtener surtidores de la estación con host e info de isla.
    Java: ObtenerInfoSurtidoresEstacionUseCase → surtidores_core
    """
    try:
        sql = """
            SELECT 
                host, 
                isla, 
                equipos_id, 
                COALESCE(atributos::json->>'surtidores', '[]') as surtidores
            FROM surtidores_core
        """
        rows = await database.fetch_all(sql)

        resultado = []
        for row in rows:
            host = str(row["host"]).strip()
            isla = row["isla"]
            equipos_id = row["equipos_id"]
            surtidores_json = row["surtidores"]

            try:
                surtidores_list = json.loads(surtidores_json) if isinstance(surtidores_json, str) else surtidores_json
            except Exception:
                surtidores_list = []

            for s in surtidores_list:
                resultado.append({
                    "surtidor": int(s),
                    "host": host,
                    "isla": isla,
                    "equipos_id": equipos_id,
                })

        print(f"[TURNOS] Surtidores estación: {len(resultado)} encontrados")
        return {"surtidores": resultado}

    except Exception as e:
        print(f"[TURNOS] Error obteniendo surtidores: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/totalizadores")
async def obtener_totalizadores(req: TotalizadorRequest):
    """
    Leer totalizadores de un surtidor vía puerto 8019.
    Java: ControllerSync.lecturasSurtidor() → POST http://{host}:8019/api/aperturaTurno
    """
    url = f"http://{req.host}:8019/api/aperturaTurno"
    body = {"surtidor": req.surtidor, "turno": 0}

    print(f"[TURNOS] Leyendo totalizadores surtidor {req.surtidor} en {url}")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                json=body,
                headers=TOTALIZADOR_HEADERS,
            )

        if response.status_code == 406:
            return {"exito": False, "mensaje": "Surtidor ocupado", "data": []}

        if response.status_code != 200:
            return {
                "exito": False,
                "mensaje": f"Error del servicio: HTTP {response.status_code}",
                "data": [],
            }

        data = response.json()
        print(f"[TURNOS] Totalizadores surtidor {req.surtidor}: {json.dumps(data, indent=2)[:500]}")

        mensaje_raw = data.get("mensajeError", "Error desconocido")
        if isinstance(mensaje_raw, str):
            # Arreglar Mojibake clásico (utf-8 parseado como windows-1252/latin1) enviado por Java
            mensaje_raw = mensaje_raw.replace("informaciÃ³n", "información")
            mensaje_raw = mensaje_raw.replace("comunicaciÃ³n", "comunicación")
            mensaje_raw = mensaje_raw.replace("Ã³", "ó").replace("Ã¡", "á").replace("Ã©", "é").replace("Ã­", "í").replace("Ãº", "ú").replace("Ã±", "ñ")

        if "codigoError" in data:
            return {
                "exito": False,
                "mensaje": mensaje_raw,
                "codigo_error": data.get("codigoError"),
                "data": [],
            }

        totalizadores = data.get("data", [])
        return {"exito": True, "data": totalizadores}

    except httpx.ConnectError:
        msg = f"No se pudo conectar al servicio de totalizadores en {url}"
        print(f"[TURNOS] {msg}")
        return {"exito": False, "mensaje": msg, "data": []}
    except Exception as e:
        print(f"[TURNOS] Error leyendo totalizadores: {e}")
        return {"exito": False, "mensaje": str(e), "data": []}


@router.post("/validar-promotor")
async def validar_promotor(req: ValidarPromotorRequest):
    """
    Validar un promotor por su identificación y PIN opcional.
    Java: FindPersonaUseCase.execute(user, password, fromRFID)
    """
    try:
        sql = """
            SELECT 
                p.id,
                p.nombre,
                p.identificacion,
                p.pin
            FROM personas p
            WHERE p.identificacion = :ident
            LIMIT 1
        """
        row = await database.fetch_one(sql, values={"ident": req.identificacion})

        if not row:
            return {"exito": False, "mensaje": "Promotor no encontrado"}

        # Si se proporcionó PIN, validarlo
        if req.pin is not None and req.pin != "":
            pin_bd = row["pin"]
            if pin_bd and str(pin_bd) != str(req.pin):
                return {"exito": False, "mensaje": "PIN incorrecto"}

        return {
            "exito": True,
            "promotor": {
                "id": int(row["id"]),
                "nombre": row["nombre"],
                "identificacion": row["identificacion"],
                "pin": row["pin"],
            },
        }

    except Exception as e:
        print(f"[TURNOS] Error validando promotor: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/iniciar")
async def iniciar_turno(req: IniciarTurnoRequest):
    """
    Iniciar turno (jornada) de un promotor.
    Java: TurnosIniciarViewController.enviarInicioTurno()
    → POST http://localhost:8010/api/jornada/iniciar
    """
    try:
        # Obtener equipos_id y empresas_id
        equipo_row = await database.fetch_one("SELECT id, empresas_id FROM equipos LIMIT 1")
        if not equipo_row:
            return {"exito": False, "mensaje": "No se encontró información del equipo"}

        equipos_id = int(equipo_row["id"])
        empresas_id = int(equipo_row["empresas_id"])

        # Verificar si ya hay un turno principal activo
        jornada_existente = await database.fetch_one(
            "SELECT grupo_jornada FROM jornadas WHERE fecha_fin IS NULL AND personas_id = :pid LIMIT 1",
            values={"pid": req.personas_id}
        )
        if jornada_existente:
            return {"exito": False, "mensaje": "Este promotor ya tiene un turno activo"}

        # Determinar si es turno principal (no hay nadie activo aún)
        turno_activo = await database.fetch_one(
            "SELECT id FROM jornadas WHERE fecha_fin IS NULL LIMIT 1"
        )
        es_principal = turno_activo is None

        # Construir JSON para LazoExpress 8010 (mismo formato que Java)
        fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        body = {
            "personas_id": req.personas_id,
            "fecha_inicio": fecha_inicio,
            "equipos_id": equipos_id,
            "empresas_id": empresas_id,
        }

        if req.surtidores:
            body["surtidores"] = req.surtidores

        atributos = {
            "saldo": req.saldo,
            "totalizadoresIniciales": None,
        }

        if es_principal and req.totalizadores:
            atributos["totalizadoresIniciales"] = req.totalizadores

        body["atributos"] = atributos
        body["ajustePeriodico"] = None

        print(f"[TURNOS] Iniciando turno para persona {req.personas_id}")
        print(f"[TURNOS] POST http://{LAZO_HOST}:8010/api/jornada/iniciar")
        print(f"[TURNOS] Body: {json.dumps(body, indent=2, default=str)[:1000]}")

        url = f"http://{LAZO_HOST}:8010/api/jornada/iniciar"

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url,
                json=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                },
            )

        print(f"[TURNOS] Response status: {response.status_code}")

        # LazoExpress retorna 200 o 201 en éxito
        if response.status_code not in (200, 201):
            texto = response.text
            print(f"[TURNOS] Error: {texto[:500]}")
            return {"exito": False, "mensaje": f"Error del servidor: {texto[:200]}"}

        data = response.json()
        print(f"[TURNOS] Response: {json.dumps(data, indent=2, default=str)[:500]}")

        # Buscar grupo_jornada/turno en la respuesta
        grupo_jornada = None
        if "data" in data and isinstance(data["data"], dict):
            grupo_jornada = data["data"].get("turno")

        if grupo_jornada is not None:
            print(f"[TURNOS] Turno iniciado OK. grupo_jornada={grupo_jornada}")
            return {
                "exito": True,
                "grupo_jornada": grupo_jornada,
                "mensaje": "Turno iniciado correctamente",
            }
        else:
            # Puede ser un mensaje informativo (ej: "SU TURNO ES ...")
            mensaje = data.get("mensaje", data.get("mensajeError", "Respuesta inesperada"))
            print(f"[TURNOS] Respuesta sin turno: {mensaje}")
            return {
                "exito": False,
                "mensaje": mensaje,
            }

    except httpx.ConnectError:
        msg = "No se pudo conectar a LazoExpress (8010)"
        print(f"[TURNOS] {msg}")
        return {"exito": False, "mensaje": msg}
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[TURNOS] Error iniciando turno: {e}")
        return {"exito": False, "mensaje": str(e)}


@router.get("/activos")
async def obtener_turnos_activos():
    """
    Obtener todos los turnos (jornadas) activos con info del promotor.
    """
    try:
        sql = """
            SELECT 
                j.id as jornada_id,
                j.grupo_jornada,
                j.personas_id,
                j.fecha_inicio,
                j.atributos,
                p.nombre as promotor_nombre,
                p.identificacion as promotor_identificacion
            FROM jornadas j
            INNER JOIN personas p ON p.id = j.personas_id
            WHERE j.fecha_fin IS NULL
            ORDER BY j.fecha_inicio DESC
        """
        rows = await database.fetch_all(sql)

        turnos = []
        for row in rows:
            atributos = row["atributos"]
            saldo = 0
            if atributos:
                try:
                    attr_dict = json.loads(atributos) if isinstance(atributos, str) else atributos
                    saldo = attr_dict.get("saldo", 0)
                except Exception:
                    pass

            turnos.append({
                "jornada_id": row["jornada_id"],
                "grupo_jornada": row["grupo_jornada"],
                "personas_id": row["personas_id"],
                "fecha_inicio": str(row["fecha_inicio"]) if row["fecha_inicio"] else None,
                "promotor_nombre": row["promotor_nombre"],
                "promotor_identificacion": row["promotor_identificacion"],
                "saldo": saldo,
            })

        print(f"[TURNOS] Turnos activos: {len(turnos)}")
        return {"turnos": turnos, "total": len(turnos)}

    except Exception as e:
        print(f"[TURNOS] Error obteniendo turnos activos: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.put("/finalizar")
async def finalizar_turno(req: FinalizarTurnoRequest):
    """
    Finalizar turno (jornada) de uno o varios promotores.
    Java: TurnosFinalizarViewController.ejecutaLoginRemoto()
    → PUT http://localhost:8010/api/jornada/finalizar
    
    Flujo:
    1. Para cada persona, obtener su jornada activa y datos
    2. Construir JSON Array con el formato que espera LazoExpress
    3. Enviar PUT al 8010
    4. Si es principal, incluir totalizadoresFinales
    """
    try:
        # Obtener equipos_id y empresas_id
        equipo_row = await database.fetch_one("SELECT id, empresas_id FROM equipos LIMIT 1")
        if not equipo_row:
            return {"exito": False, "mensaje": "No se encontró información del equipo"}

        equipos_id = int(equipo_row["id"])
        empresas_id = int(equipo_row["empresas_id"])
        fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Determinar quién es el principal (el primero con turno activo)
        principal_row = await database.fetch_one(
            "SELECT personas_id FROM jornadas WHERE fecha_fin IS NULL ORDER BY fecha_inicio ASC LIMIT 1"
        )
        principal_id = int(principal_row["personas_id"]) if principal_row else None

        # Construir array de personas para el cierre
        personas_cierre = []

        for per in req.personas:
            # Buscar la jornada activa de esta persona
            jornada_row = await database.fetch_one(
                """SELECT id, grupo_jornada, personas_id, atributos
                   FROM jornadas 
                   WHERE personas_id = :pid AND fecha_fin IS NULL 
                   LIMIT 1""",
                values={"pid": per.personas_id}
            )

            if not jornada_row:
                print(f"[TURNOS] Persona {per.personas_id} no tiene turno activo, omitiendo")
                continue

            # IMPORTANTE: Java usa grupo_jornada como identificadorJornada, NO jornada.id
            # Java: per.getGrupoJornadaId() → campo grupo_jornada de la tabla jornadas
            grupo_jornada = jornada_row["grupo_jornada"]
            id_jornada = int(grupo_jornada) if grupo_jornada else per.grupo_jornada

            print(f"[TURNOS] Persona {per.personas_id}: jornada.id={jornada_row['id']}, grupo_jornada={grupo_jornada} -> identificadorJornada={id_jornada}")

            # Construir objeto de cierre (mismo formato que Java)
            cierre_obj = {
                "personas_id": per.personas_id,
                "identificadorJornada": id_jornada,
                "fecha_fin": fecha_fin,
                "equipos_id": equipos_id,
                "empresas_id": empresas_id,
                "atributos": {
                    "venta_total": 0,
                },
            }

            # Solo el principal lleva totalizadoresFinales y ajustePeriodico
            es_este_principal = (per.personas_id == principal_id) if principal_id else False
            if es_este_principal and req.totalizadoresFinales:
                cierre_obj["atributos"]["totalizadoresFinales"] = req.totalizadoresFinales
                cierre_obj["ajustePeriodico"] = None

            personas_cierre.append(cierre_obj)

        if not personas_cierre:
            return {"exito": False, "mensaje": "No se encontraron turnos activos para cerrar"}

        # Persona principal (primer turno que se inició)
        persona_principal = personas_cierre[0] if personas_cierre else None

        print(f"[TURNOS] Cerrando turno para {len(personas_cierre)} persona(s)")

        url = f"http://{LAZO_HOST}:8010/api/jornada/finalizar"

        # ─── Intento 1: POST con cierreEstacion ───
        # Java TurnoMenuPanelController usa POST con {cierreEstacion:true}
        # cuando WITH_JAVA_CONTROL != 'N' (turnos automáticos con controlador externo)
        cierre_body = {
            "cierreEstacion": True,
            "promotorId": persona_principal["personas_id"] if persona_principal else 0,
            "promotor": "",
            "informar": 1,
        }

        print(f"[TURNOS] Intento 1: POST {url} con cierreEstacion=true")
        print(f"[TURNOS] Body: {json.dumps(cierre_body, indent=2)}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url,
                json=cierre_body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                },
            )

        print(f"[TURNOS] Intento 1 response status: {response.status_code}")

        if response.status_code in (200, 201):
            data = response.json()
            print(f"[TURNOS] Finalizar response: {json.dumps(data, indent=2, default=str)[:500]}")
            return {
                "exito": True,
                "mensaje": data.get("mensaje", "Turno finalizado correctamente"),
                "data": data,
            }

        # ─── Intento 2: PUT con array (modo sin turnos automáticos) ───
        print(f"[TURNOS] Intento 1 falló ({response.status_code}), probando PUT con array...")
        print(f"[TURNOS] PUT {url}")
        print(f"[TURNOS] Body: {json.dumps(personas_cierre, indent=2, default=str)[:1500]}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            response2 = await client.put(
                url,
                json=personas_cierre,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                },
            )

        print(f"[TURNOS] Intento 2 response status: {response2.status_code}")

        if response2.status_code in (200, 201):
            data = response2.json()
            print(f"[TURNOS] Finalizar response: {json.dumps(data, indent=2, default=str)[:500]}")
            return {
                "exito": True,
                "mensaje": data.get("mensaje", "Turno finalizado correctamente"),
                "data": data,
            }

        # Ambos intentos fallaron
        texto = response2.text
        print(f"[TURNOS] Ambos intentos fallaron. Último error: {texto[:500]}")
        return {"exito": False, "mensaje": f"Error del servidor: {texto[:200]}"}

    except httpx.ConnectError:
        msg = "No se pudo conectar a LazoExpress (8010)"
        print(f"[TURNOS] {msg}")
        return {"exito": False, "mensaje": msg}
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[TURNOS] Error finalizando turno: {e}")
        return {"exito": False, "mensaje": str(e)}
