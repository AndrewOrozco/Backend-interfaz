"""
Conexión a las bases de datos PostgreSQL
Usando databases para operaciones async

Java tiene 2 bases de datos:
  - lazoexpresscore:    ct_movimientos, ct_medios_pagos, ct_bodegas_productos, ...
  - lazoexpressregistry: productos, bodegas_productos, grupos, medios_pagos, ...
"""
from databases import Database
from app.config import settings

# Base de datos core (ct_movimientos, ventas, etc.)
database = Database(settings.database_url)

# Base de datos registry (productos, bodegas, grupos, canastilla)
database_registry = Database(settings.database_registry_url)

async def get_db():
    """Dependency para obtener la conexión a la DB core"""
    return database
