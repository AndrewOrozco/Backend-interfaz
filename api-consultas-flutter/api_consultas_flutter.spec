# PyInstaller spec para api-consultas-flutter
# Uso (con .venv activado): pyinstaller api_consultas_flutter.spec

block_cipher = None

# Imports que PyInstaller no detecta solo
hidden_imports = [
    # ── Uvicorn ──────────────────────────────────────────────
    'uvicorn.logging',
    'uvicorn.loops',
    'uvicorn.loops.auto',
    'uvicorn.loops.asyncio',
    'uvicorn.protocols',
    'uvicorn.protocols.http',
    'uvicorn.protocols.http.auto',
    'uvicorn.protocols.http.h11_impl',
    'uvicorn.protocols.websockets',
    'uvicorn.protocols.websockets.auto',
    'uvicorn.protocols.websockets.websockets_impl',
    'uvicorn.lifespan',
    'uvicorn.lifespan.on',
    'uvicorn.main',
    # ── databases (usa import_from_string → PyInstaller no lo ve) ──
    'databases',
    'databases.core',
    'databases.backends',
    'databases.backends.asyncpg',
    'databases.backends.aiopg',
    'databases.backends.aiomysql',
    'databases.backends.postgres',   # ← URL scheme postgresql → mapea a este módulo
    'databases.backends.mysql',
    'databases.backends.sqlite',
    # ── asyncpg (C extension, varios sub-módulos necesarios) ──────
    'asyncpg',
    'asyncpg.pgproto',
    'asyncpg.pgproto.pgproto',
    'asyncpg.protocol',
    'asyncpg.protocol.protocol',
    # ── HTTP / Pydantic / Dotenv ──────────────────────────────────
    'httpx',
    'pydantic',
    'pydantic_settings',
    'dotenv',
    'websockets',
    'websockets.legacy',
    'websockets.legacy.server',
]

a = Analysis(
    ['run.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# Ejecutable: carpeta (onedir). Más estable para FastAPI/uvicorn.
# Salida: dist\api_consultas_flutter\api_consultas_flutter.exe + DLLs
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='api_consultas_flutter',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='api_consultas_flutter',
)
