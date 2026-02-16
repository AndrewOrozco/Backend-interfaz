"""
Script para ejecutar la API
"""
import uvicorn
from app.config import settings

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  API CONSULTAS FLUTTER - FastAPI")
    print("=" * 60)
    print(f"  Host: {settings.APP_HOST}")
    print(f"  Puerto: {settings.APP_PORT}")
    print(f"  Base de datos: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
    print("=" * 60)
    print("  Documentación:")
    print(f"    Swagger: http://localhost:{settings.APP_PORT}/docs")
    print(f"    ReDoc:   http://localhost:{settings.APP_PORT}/redoc")
    print("=" * 60 + "\n")
    
    uvicorn.run(
        "app.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.DEBUG
    )
