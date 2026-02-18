"""
Punto de entrada para ejecutar la API (útil para PyInstaller).
"""
if __name__ == "__main__":
    import uvicorn
    from app.main import app
    uvicorn.run(app, host="0.0.0.0", port=8020)
