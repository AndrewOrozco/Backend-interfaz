# Generar ejecutable (Windows)

El proyecto usa un **.venv** con todas las dependencias. Para sacar el ejecutable:

## Opción 1: Script automático (recomendado)

1. Abre **PowerShell** o **CMD** en la carpeta del proyecto (`api-consultas-flutter`).
2. Ejecuta:
   ```bat
   build.bat
   ```
3. El ejecutable quedará en:
   ```
   dist\api_consultas_flutter\api_consultas_flutter.exe
   ```
   Debes **copiar toda la carpeta** `dist\api_consultas_flutter` al equipo donde vayas a correr la API (el .exe necesita los archivos que están junto a él).

## Opción 2: Paso a paso manual

1. Activar el .venv:
   ```bat
   .venv\Scripts\activate
   ```
2. Instalar PyInstaller (solo la primera vez):
   ```bat
   pip install pyinstaller
   ```
3. Generar el ejecutable:
   ```bat
   pyinstaller api_consultas_flutter.spec
   ```
4. Resultado: carpeta `dist\api_consultas_flutter\` con `api_consultas_flutter.exe` y el resto de archivos necesarios.

## Cómo usar el ejecutable

- En el equipo destino, copia la **carpeta completa** `dist\api_consultas_flutter`.
- Ejecuta `api_consultas_flutter.exe`.
- La API quedará escuchando en **http://0.0.0.0:8020** (mismo puerto que en desarrollo).
- **Importante:** En ese equipo deben estar configuradas las variables de entorno o el `.env` (conexión a PostgreSQL, etc.), o el .exe leerá lo que haya en el sistema. Si usas un .env en desarrollo, tendrás que configurar en el equipo destino (o copiar un .env junto al .exe y que tu app cargue el path correcto).

## Si algo falla al ejecutar el .exe

- Faltan **hidden imports**: edita `api_consultas_flutter.spec`, añade el módulo en `hiddenimports`, y vuelve a ejecutar `pyinstaller api_consultas_flutter.spec`.
- Si prefieres **un solo .exe** (sin carpeta), en el .spec se puede cambiar a modo "onefile"; el arranque suele ser un poco más lento.
