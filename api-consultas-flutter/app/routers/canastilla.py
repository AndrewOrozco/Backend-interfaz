"""
Router para consultas de canastilla/productos
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from app.database import database
from app.models.schemas import ListaProductos, Producto

router = APIRouter()

@router.get("/productos", response_model=ListaProductos)
async def obtener_productos(
    categoria: Optional[str] = Query(None, description="Filtrar por categoría"),
    activos: bool = Query(True, description="Solo productos activos"),
    buscar: Optional[str] = Query(None, description="Buscar por descripción o código")
):
    """
    Obtener lista de productos de canastilla
    
    Parámetros:
    - categoria: Filtrar por categoría
    - activos: Solo mostrar productos activos (default: True)
    - buscar: Buscar por descripción o código
    """
    try:
        filtros = []
        params = {}
        
        if activos:
            filtros.append("p.estado = 'A'")
        
        if categoria:
            filtros.append("pf.descripcion ILIKE :categoria")
            params['categoria'] = f"%{categoria}%"
            
        if buscar:
            filtros.append("(p.descripcion ILIKE :buscar OR p.codigo ILIKE :buscar)")
            params['buscar'] = f"%{buscar}%"
        
        where_clause = " AND ".join(filtros) if filtros else "1=1"
        
        query = f"""
            SELECT 
                p.id,
                p.codigo,
                p.descripcion,
                p.precio_venta as precio,
                COALESCE(b.cantidad, 0) as stock,
                COALESCE(pf.descripcion, 'Sin categoría') as categoria,
                CASE WHEN p.estado = 'A' THEN true ELSE false END as activo
            FROM productos p
            LEFT JOIN productos_familias pf ON pf.id = p.productos_familias_id
            LEFT JOIN (
                SELECT productos_id, SUM(cantidad) as cantidad 
                FROM ct_bodegas_productos 
                GROUP BY productos_id
            ) b ON b.productos_id = p.id
            WHERE {where_clause}
            ORDER BY p.descripcion
            LIMIT 500
        """
        
        rows = await database.fetch_all(query, params)
        
        productos = []
        for row in rows:
            productos.append(Producto(
                id=row['id'],
                codigo=row['codigo'],
                descripcion=row['descripcion'],
                precio=row['precio'],
                stock=int(row['stock'] or 0),
                categoria=row['categoria'],
                activo=row['activo']
            ))
        
        return ListaProductos(
            total=len(productos),
            productos=productos
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando productos: {str(e)}")


@router.get("/categorias")
async def obtener_categorias():
    """
    Obtener lista de categorías de productos
    """
    try:
        query = """
            SELECT 
                pf.id,
                pf.descripcion,
                pf.codigo,
                COUNT(p.id) as total_productos
            FROM productos_familias pf
            LEFT JOIN productos p ON p.productos_familias_id = pf.id AND p.estado = 'A'
            WHERE pf.estado = 'A'
            GROUP BY pf.id, pf.descripcion, pf.codigo
            ORDER BY pf.descripcion
        """
        
        rows = await database.fetch_all(query)
        
        return {
            "total": len(rows),
            "categorias": [dict(row) for row in rows]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando categorías: {str(e)}")
