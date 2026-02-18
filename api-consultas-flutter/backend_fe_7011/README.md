# Backend FE 7011 – Ajuste para Flutter

Módulo para que el **backend Python** (api-consultas-flutter) envíe ventas a **7011** (facturación electrónica) y dispare la impresión en LazoExpress.

## Qué hace

1. **Venta pump (Flutter)**  
   Cuando la venta se gestiona desde Flutter (status pump), el backend:
   - Envía el payload a **7011** (`EnviarDatosMovimientoDian`).
   - Tras respuesta OK, dispara la **impresión** de la factura en LazoExpress (ya con CUFE).

2. **Ventas sin resolver**  
   Cuando el usuario asigna cliente y envía:
   - El backend envía a **7011** en ese momento.
   - Al imprimir desde historial, la factura ya tiene CUFE (impresión rápida).

## Instalación en api-consultas-flutter

1. **Copiar la carpeta**  
   La carpeta `backend_fe_7011` ya está en este proyecto.

2. **Dependencia**  
   En tu `requirements.txt`:
   ```
   httpx>=0.24.0
   ```

3. **Variables de entorno** (opcional; si no, se usan defaults):

   | Variable | Descripción | Ejemplo |
   |----------|-------------|--------|
   | `FE_7011_HOST` | Host del servicio 7011 | `servicio.terpelpos.com` |
   | `FE_7011_PORT` | Puerto | `7011` |
   | `FE_7011_SSL` | Usar HTTPS | `true` |
   | `FE_7011_TOKEN` | Token Bearer (si lo usa el 7011) | |
   | `FE_7011_PASSWORD` | Password (si lo usa el 7011) | |
   | `FE_7011_DEVICE_ID` | identificadorDispositivo | `backend-flutter` |
   | `LAZOEXPRESS_URL` | URL de LazoExpress para imprimir | `http://127.0.0.1:7010` |

4. **Registrar el router en FastAPI**  

   En tu `main.py` (o donde crees la app):

   ```python
   from fastapi import FastAPI

   from backend_fe_7011.routers import fe_7011_router

   app = FastAPI()
   app.include_router(fe_7011_router.router)
   ```

## Endpoints

### POST `/ventas/enviar-fe-pump`

Para venta gestionada desde Flutter (status pump): envía a 7011 e imprime.

**Body:**
```json
{
  "payload_fe": { "venta": { ... }, "cliente": { ... }, "identificadorMovimiento": 123 },
  "identificador_movimiento": 123,
  "imprimir_despues": true,
  "tipo_reporte": "FACTURA-ELECTRONICA"
}
```

### POST `/ventas/sin-resolver/resolver-y-enviar-fe`

Para ventas sin resolver: al asignar cliente y enviar, envía a 7011 de inmediato.

**Body:**
```json
{
  "payload_fe": { "venta": { ... }, "cliente": { ... } },
  "identificador_movimiento": 456,
  "imprimir_despues": false
}
```

## Flutter: cuándo llamar

- **Venta pump (gestionada con cliente):**  
  Tras registrar la venta en backend/LazoExpress con `statusPump: true`, llamar a `POST /ventas/enviar-fe-pump` con el `payload_fe` y `identificador_movimiento`. El backend enviará a 7011 y disparará la impresión.

- **Ventas sin resolver:**  
  En la pantalla "ventas sin resolver", cuando el usuario asigna cliente y pulsa "Enviar", llamar a `POST /ventas/sin-resolver/resolver-y-enviar-fe` con el payload de la venta (ya con cliente) y el `identificador_movimiento`. El backend enviará a 7011 en ese momento; luego en historial la factura ya tendrá CUFE.

## Logs [FE-7011]

El módulo escribe logs con prefijo `[FE-7011]` para ver en consola: consulta cliente, envío a 7011, CUFE y disparo de impresión.
