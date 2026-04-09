from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

class SupervisorAuthRequest(BaseModel):
    username: str
    password: str
    modulo: Optional[str] = None # Para saber si es de 'ANULACIONES' o 'VENTA MANUAL' por auditoría

class SupervisorAuthResponse(BaseModel):
    exito: bool
    mensaje: str
    supervisor_id: Optional[int] = None
    es_admin: bool = False

@router.post("/autorizar-supervisor", response_model=SupervisorAuthResponse)
async def autorizar_supervisor(req: SupervisorAuthRequest):
    """
    Valida las credenciales de un supervisor administrativo.
    Se utiliza para dar permisos en Anulaciones, Consumo Propio, Venta Manual, etc.
    """
    # TODO: Conectar con base de datos (tbl_usuario, tbl_persona) para validar hash
    # Mocking temporal de autorización según la ISO 27001 para la arquitectura inicial:
    
    # Usuario mock (Lógica temporal para pruebas)
    if req.username == "admin" and req.password == "1234":
        return SupervisorAuthResponse(
            exito=True,
            mensaje="Autorización exitosa",
            supervisor_id=1,
            es_admin=True
        )
    
    raise HTTPException(status_code=401, detail="Credenciales incorrectas o usuario no es Administrador")
