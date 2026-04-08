"""
Módulo para cachear URLs globales de servicios externos.

Obtiene el host desde whacer_parametros.direccion y lo mantiene en caché
para evitar consultas repetidas a la base de datos.
"""
import httpx
from typing import Optional

# Cache del host
_host_cache: Optional[str] = None

async def get_host_from_db(database) -> str:
    """
    Obtiene el host desde whacer_parametros.direccion en la BD.
    
    Returns:
        str: El host/dirección configurado
    """
    global _host_cache
    
    # Si ya está en caché, retornarlo
    if _host_cache is not None:
        return _host_cache
    
    try:
        query = """
            SELECT valor
            FROM public.wacher_parametros wp
            WHERE wp.codigo = 'HOST_SERVER';
        """
        row = await database.fetch_one(query)
        
        if row and row['valor']:
            _host_cache = row['valor']
            print(f"[URL_GLOBAL] Host cacheado: {_host_cache}")
            return _host_cache
        else:
            # Fallback por defecto
            _host_cache = "127.0.0.1"
            print(f"[URL_GLOBAL] Usando host por defecto: {_host_cache}")
            return _host_cache
            
    except Exception as e:
        print(f"[URL_GLOBAL] Error obteniendo host: {e}")
        _host_cache = "127.0.0.1"
        return _host_cache


def get_cached_host() -> Optional[str]:
    """Retorna el host cacheado (puede ser None si no se ha inicializado)"""
    return _host_cache


def clear_cache():
    """Limpia la caché del host"""
    global _host_cache
    _host_cache = None
    print("[URL_GLOBAL] Cache limpiado")


def build_url(host: str, port: int, path: str) -> str:
    """
    Construye una URL HTTPS con el host, puerto y path dados.
    
    Args:
        host: El host/IP
        port: El puerto
        path: El path del endpoint (sin / inicial)
        
    Returns:
        str: URL completa https://host:port/path
    """
    # Limpiar path
    if path.startswith('/'):
        path = path[1:]
    
    return f"https://{host}:{port}/{path}"


# URLs de servicios específicos
class ServiciosTerpel:
    """URLs de los servicios de Terpel"""
    
    PUERTO_CONSULTA_CLIENTE = 7011
    PATH_CONSULTA_CLIENTE = "proxi.terpel/consultarCliente"
    
    # Orquestador de pagos (APP TERPEL, etc.)
    PUERTO_ORQUESTADOR_PAGOS = 5555
    PATH_ORQUESTADOR_PAGOS = "v1/payments/"
    
    @classmethod
    def url_consultar_cliente(cls, host: str) -> str:
        """URL para consultar cliente"""
        return build_url(host, cls.PUERTO_CONSULTA_CLIENTE, cls.PATH_CONSULTA_CLIENTE)

    @classmethod
    def url_base_7011(cls, host: str) -> str:
        """URL base del servicio 7011 (FE). Para usar con backend_fe_7011."""
        return build_url(host, cls.PUERTO_CONSULTA_CLIENTE, "").rstrip("/")
    
    @classmethod
    def url_orquestador_pagos(cls) -> str:
        """URL del orquestador de pagos (siempre localhost)"""
        return f"http://localhost:{cls.PUERTO_ORQUESTADOR_PAGOS}/{cls.PATH_ORQUESTADOR_PAGOS}"


def get_terpel_headers() -> dict:
    """
    Obtiene los headers requeridos para las APIs de Terpel.
    """
    from datetime import datetime, timezone
    
    fecha_actual = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    
    return {
        "aplicacion": "21.4.1",
        "content-Type": "application/json",
        "fecha": fecha_actual,
        "identificadorDispositivo": "null-NO TIENE",
        "versionApp": "TERPEL",
        "versionCode": "21.4.1"
    }


async def consultar_cliente_externo(
    database,
    documento_cliente: str,
    tipo_documento: int
) -> dict:
    """
    Consulta un cliente en el servicio externo de Terpel.
    
    Args:
        database: Conexión a la BD para obtener el host
        documento_cliente: Número de documento del cliente
        tipo_documento: Código del tipo de documento
        
    Returns:
        dict: Respuesta del servicio o datos por defecto
    """
    try:
        host = await get_host_from_db(database)
        url = ServiciosTerpel.url_consultar_cliente(host)
        
        headers = get_terpel_headers()
        
        body = {
            "documentoCliente": documento_cliente,
            "tipoDocumentoCliente": tipo_documento
        }
        
        print(f"[URL_GLOBAL] Consultando cliente en: {url}")
        print(f"[URL_GLOBAL] Headers: {headers}")
        print(f"[URL_GLOBAL] Body: {body}")
        
        # SECURITY NOTE (ISO 27001 / CWE-295): El servicio Terpel usa HTTPS con
        # certificado autofirmado interno. Cuando esté disponible el certificado CA
        # corporativo, reemplazar verify=False por verify="/ruta/al/terpel_ca.pem"
        # TODO: Obtener certificado CA desde configuración o variable de entorno TERPEL_CA_CERT
        async with httpx.AsyncClient(verify=False, timeout=15.0) as client:  # nosec B501
            response = await client.post(url, json=body, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                print(f"[URL_GLOBAL] Respuesta cliente: {data}")
                return {
                    "success": True,
                    "data": data
                }
            else:
                print(f"[URL_GLOBAL] Error HTTP {response.status_code}: {response.text}")
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "data": None
                }
                
    except httpx.TimeoutException:
        print("[URL_GLOBAL] Timeout consultando cliente")
        return {
            "success": False,
            "error": "Timeout",
            "data": None
        }
    except Exception as e:
        print(f"[URL_GLOBAL] Error consultando cliente: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": None
        }
