# API Consultas Flutter

Backend FastAPI para consultas del frontend Flutter POS.

## Características

- **Ventas**: Historial y ventas sin resolver
- **Surtidores**: Estados y configuración
- **Canastilla**: Productos y categorías
- **Configuración**: EDS, promotores, parámetros

## Instalación

1. Crear entorno virtual:
```bash
python -m venv venv
```

2. Activar entorno:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. Instalar dependencias:
```bash
pip install -r requirements.txt
```

4. Configurar base de datos:
```bash
# Copiar .env.example a .env
copy .env.example .env

# Editar .env con tus credenciales de PostgreSQL
```

## Ejecución

```bash
# Desarrollo
uvicorn app.main:app --reload --host 0.0.0.0 --port 8020

# Producción
uvicorn app.main:app --host 0.0.0.0 --port 8020
```

## Documentación API

Una vez corriendo, accede a:
- Swagger UI: http://localhost:8020/docs
- ReDoc: http://localhost:8020/redoc

## Endpoints

### Ventas
- `GET /ventas/sin-resolver` - Ventas pendientes
- `GET /ventas/historial` - Historial con paginación
- `GET /ventas/resumen` - Resumen del día

### Surtidores
- `GET /surtidores/estados` - Estados actuales
- `GET /surtidores/configuracion` - Configuración

### Canastilla
- `GET /canastilla/productos` - Lista de productos
- `GET /canastilla/categorias` - Categorías

### Configuración
- `GET /configuracion/eds` - Datos de la EDS
- `GET /configuracion/promotores` - Promotores activos
- `GET /configuracion/parametros` - Parámetros del sistema
- `GET /configuracion/medios-pago` - Medios de pago

## Puerto

Puerto por defecto: **8020**

(Para no chocar con LazoExpress:8010 ni Flask:5000)
