"""
Schemas Pydantic para validación y serialización
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

# ============================================
# VENTAS
# ============================================

class VentaBase(BaseModel):
    """Schema base para una venta"""
    id: int
    fecha: Optional[datetime] = None
    monto_total: Optional[Decimal] = None
    volumen_total: Optional[Decimal] = None
    estado: Optional[str] = None
    tipo: Optional[str] = None
    cara: Optional[int] = None
    manguera: Optional[int] = None
    producto: Optional[str] = None
    promotor: Optional[str] = None
    sincronizado: Optional[bool] = None

class VentaSinResolver(BaseModel):
    """Schema para ventas sin resolver (pendientes)"""
    id: int
    fecha: Optional[datetime] = None
    monto_total: Optional[Decimal] = None
    tipo_venta: Optional[str] = None  # combustible, canastilla
    estado_sync: Optional[str] = None  # pendiente_fe, pendiente_datafono, etc
    descripcion: Optional[str] = None
    cara: Optional[int] = None
    producto: Optional[str] = None

class VentaHistorial(BaseModel):
    """Schema para historial de ventas"""
    id: int
    fecha: Optional[datetime] = None
    fecha_formateada: Optional[str] = None
    monto_total: Optional[Decimal] = None
    volumen_total: Optional[Decimal] = None
    producto: Optional[str] = None
    cara: Optional[int] = None
    manguera: Optional[int] = None
    promotor: Optional[str] = None
    cliente: Optional[str] = None
    placa: Optional[str] = None
    tipo_pago: Optional[str] = None
    estado: Optional[str] = None

class ListaVentasSinResolver(BaseModel):
    """Respuesta con lista de ventas sin resolver"""
    total: int
    ventas: List[VentaSinResolver]

class ListaVentasHistorial(BaseModel):
    """Respuesta con historial de ventas paginado"""
    total: int
    pagina: int
    por_pagina: int
    ventas: List[VentaHistorial]

# ============================================
# SURTIDORES
# ============================================

class EstadoSurtidor(BaseModel):
    """Estado de un surtidor"""
    id: int
    cara: int
    manguera: Optional[int] = None
    estado: str
    estado_codigo: int
    producto: Optional[str] = None
    precio_unitario: Optional[Decimal] = None

class ListaEstadosSurtidores(BaseModel):
    """Lista de estados de surtidores"""
    total: int
    surtidores: List[EstadoSurtidor]

# ============================================
# CANASTILLA / PRODUCTOS
# ============================================

class Producto(BaseModel):
    """Schema para un producto"""
    id: int
    codigo: Optional[str] = None
    descripcion: str
    precio: Optional[Decimal] = None
    stock: Optional[int] = None
    categoria: Optional[str] = None
    activo: bool = True

class ListaProductos(BaseModel):
    """Lista de productos"""
    total: int
    productos: List[Producto]

# ============================================
# CONFIGURACIÓN
# ============================================

class ConfiguracionEDS(BaseModel):
    """Configuración de la EDS"""
    id: int
    nombre: str
    nit: Optional[str] = None
    direccion: Optional[str] = None
    telefono: Optional[str] = None
    isla: Optional[int] = None

# ============================================
# RESPUESTAS GENERALES
# ============================================

class ResponseOK(BaseModel):
    """Respuesta exitosa genérica"""
    success: bool = True
    message: str
    data: Optional[dict] = None

class ResponseError(BaseModel):
    """Respuesta de error"""
    success: bool = False
    error: str
    detail: Optional[str] = None
