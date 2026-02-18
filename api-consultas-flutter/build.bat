@echo off
REM Generar ejecutable con PyInstaller (usa .venv del proyecto)
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo No se encuentra .venv. Crea uno con: python -m venv .venv
    exit /b 1
)

echo Activando .venv...
call .venv\Scripts\activate.bat

echo Instalando PyInstaller si no esta...
pip install pyinstaller -q

echo Construyendo ejecutable...
pyinstaller api_consultas_flutter.spec

if %ERRORLEVEL% equ 0 (
    echo.
    echo Listo. Ejecutable en: dist\api_consultas_flutter\api_consultas_flutter.exe
    echo Ejecuta ese .exe (o toda la carpeta dist\api_consultas_flutter) en el equipo destino.
) else (
    echo Error en la construccion.
    exit /b 1
)
