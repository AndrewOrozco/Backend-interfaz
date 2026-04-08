"""
Configuración de la aplicación
"""
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    """Configuración de la aplicación usando variables de entorno"""
    
    # Base de datos PostgreSQL (core = ct_movimientos, ventas, etc.)
    # IMPORTANTE: debe ser "lazoexpresscore" para coincidir con Java y el print service
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "lazoexpresscore"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    
    # Base de datos Registry (productos, bodegas_productos, grupos, etc.)
    DB_REGISTRY_NAME: str = "lazoexpressregistry"
    
    # Configuración del servidor
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8020
    DEBUG: bool = True
    
    # Servicio de impresión Python y LazoExpress Java
    PRINT_SERVICE_URL: str = "http://127.0.0.1:8001"
    LAZOEXPRESS_URL: str = "http://127.0.0.1:8010"
    
    # Host del servidor central LazoExpress (para proxying a 8010, 8014, etc.)
    LAZO_HOST: str = "localhost"

    # ── Sistema de Licencias (HO) ─────────────────────────────────────────────
    # URL del servidor de la HO que valida los códigos numéricos de licencia.
    LICENSE_SERVER_URL: str = "http://127.0.0.1:8012/v1.0/mqtt/register"
    # Header de autenticación para el servidor HO (igual que el sistema Java).
    LICENSE_AUTH_HEADER: str = "Basic cGFzc3BvcnR4OlQ0MUFYUWtYSjh6"
    # Solo para desarrollo local — dejar vacío en producción
    LICENSE_MOCK_CODE: str = ""

    # Token para API Externa de Fidelización (evitar hardcoding)
    FIDELIZACION_TOKEN: str = "Basic cGFzc3BvcnR4OlQ0MUFYUWtYSjh6"

    # ── Gilbarco Core ─────────────────────────────────────────────────────────
    # API local/remota de Gilbarco para saltos de lectura y otros comandos
    GILBARCO_CORE_URL: str = "http://127.0.0.1:8000/api"


    @property
    def database_url(self) -> str:
        """Construir URL de conexión a PostgreSQL usando asyncpg (core)"""
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def database_registry_url(self) -> str:
        """Construir URL de conexión a PostgreSQL Registry usando asyncpg"""
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_REGISTRY_NAME}"
    
    @property
    def async_database_url(self) -> str:
        """URL para conexión async con asyncpg"""
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

@lru_cache()
def get_settings() -> Settings:
    """Obtener configuración (cached)"""
    return Settings()

settings = get_settings()
