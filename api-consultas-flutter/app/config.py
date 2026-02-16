"""
Configuración de la aplicación
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    """Configuración de la aplicación usando variables de entorno"""
    
    # Base de datos PostgreSQL
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "lazo_express"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    
    # Configuración del servidor
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8020
    DEBUG: bool = True
    
    # Host del servidor central LazoExpress (para proxying a 8010, 8014, etc.)
    LAZO_HOST: str = "localhost"
    
    @property
    def database_url(self) -> str:
        """Construir URL de conexión a PostgreSQL usando aiopg"""
        return f"postgresql+aiopg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
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
