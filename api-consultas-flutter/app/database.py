"""
Conexión a la base de datos PostgreSQL
Usando databases para operaciones async
"""
from databases import Database
from app.config import settings

# Crear instancia de la base de datos
database = Database(settings.database_url)

async def get_db():
    """Dependency para obtener la conexión a la DB"""
    return database
