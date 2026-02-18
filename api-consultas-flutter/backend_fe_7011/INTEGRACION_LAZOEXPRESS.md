# Integración FE 7011 en LazoExpress

LazoExpress hace la inserción en `ct_movimientos` y la **impresión automática**. La idea: en el punto donde LazoExpress **valida si debe imprimir automáticamente** (factura electrónica pump), que **llame al backend**; el backend se encarga del 7011 y de la impresión.

---

## Punto de integración en LazoExpress

**Dónde:** Donde LazoExpress valida / decide la **impresión automática** (p. ej. “¿imprimir factura electrónica para esta venta?”).

**Qué hacer ahí:**

1. Si la venta tiene **factura electrónica** y **statusPump** (atributos ya guardados en `ct_movimientos`):
   - **No** disparar vosotros la impresión todavía.
   - Llamar al **backend** (api-consultas-flutter) con el payload de la venta.
2. El backend:
   - Envía al **7011** (usa URL_GLOBAL desde `wacher_parametros`).
   - Si todo va bien, dispara la **impresión** (en 8001 print-ticket o en LazoExpress, según configuración).
3. Si el backend responde **200 OK** y enviasteis `imprimir_despues: true`:
   - **No** volver a imprimir desde LazoExpress (ya lo hizo el backend).
4. Si el backend responde **error**:
   - Decidir en LazoExpress: mostrar error, reintentar o no imprimir.

Resumen: **LazoExpress solo llama al backend en el punto de “validar impresión automática”; el backend hace 7011 + impresión.**

---

## Opción A: LazoExpress llama a api-consultas-flutter (recomendada)

LazoExpress no implementa el cliente 7011; solo llama a la API de Flutter, que ya tiene URL_GLOBAL y el cliente 7011.

### Cuándo

En el flujo donde LazoExpress:

1. Inserta/actualiza `ct_movimientos` con los atributos (incluido `factura_electronica` y `statusPump`).
2. Tiene el `identificadorMovimiento` (id del movimiento).
3. **Valida si debe hacer impresión automática** → justo ahí, **antes** de imprimir:

1. Construir `payload_fe` con:
   - `venta`: `{ "identificadorMovimiento": <id> }` (y cualquier otro campo que ya envíen al 7011).
   - `cliente`: el objeto completo de `atributos.factura_electronica` (numeroDocumento, tipoDocumento, nombreComercial, etc.).
   - `identificadorMovimiento`: mismo id.

2. Llamar a la API de consultas Flutter:

   **POST** `http://<host-api-consultas>:8020/ventas/enviar-fe-pump`

   **Body (JSON):**
   ```json
   {
     "payload_fe": {
       "venta": { "identificadorMovimiento": 123 },
       "cliente": { "numeroDocumento": "...", "tipoDocumento": 13, "nombreComercial": "...", ... },
       "identificadorMovimiento": 123
     },
     "identificador_movimiento": 123,
     "imprimir_despues": true,
     "tipo_reporte": "FACTURA-ELECTRONICA"
   }
   ```

3. Si la respuesta es OK (200), la API ya habrá:
   - Enviado la venta al **7011** (usando el host de URL_GLOBAL / `wacher_parametros`).
   - Disparado la impresión (por defecto contra `LAZOEXPRESS_URL`, p. ej. `http://127.0.0.1:7010` → `/api/imprimir/FACTURA-ELECTRONICA`).

4. En LazoExpress:
   - Si `imprimir_despues: true` y la API respondió OK, **no** volver a llamar a vuestra impresión automática (ya la hizo la API).
   - Si preferís que la impresión la siga haciendo solo LazoExpress, llamad con `imprimir_despues: false` y después disparad vosotros la impresión como hasta ahora.

### Configuración en api-consultas-flutter

- La URL del 7011 sale de **URL_GLOBAL** (tabla `wacher_parametros`, código `HOST_SERVER`).
- Para que la API dispare la impresión en vuestro servicio (p. ej. puerto 8001), configurad en el entorno donde corre la API:
  - `LAZOEXPRESS_URL=http://127.0.0.1:8001` (o la URL que use LazoExpress para imprimir).
  - Si el servicio de impresión en 8001 espera otro path (p. ej. `/print-ticket/sales`), puede ser necesario ajustar `fe_flow._disparar_impresion` en `backend_fe_7011` para que llame a ese endpoint.

---

## Opción B: LazoExpress implementa el cliente 7011

LazoExpress obtiene el host del 7011 (igual que consultar cliente) y llama directamente al 7011.

### Cuándo

Justo después de insertar en `ct_movimientos` y antes de la impresión automática, cuando `atributos.factura_electronica` y `atributos.statusPump` estén presentes.

### Pasos en LazoExpress

1. **Obtener URL base del 7011**
   - Misma lógica que para “consultar cliente”: leer `wacher_parametros` (código `HOST_SERVER`) → `host`.
   - URL base: `https://<host>:7011`.

2. **Opcional: consultar cliente en 7011**
   - **POST** `https://<host>:7011/proxi.terpel/consultarCliente`
   - Body: `{ "numeroDocumento": "<doc>", "tipoDocumento": <tipo> }`
   - Headers: `Content-Type: application/json`, `fecha` (ISO), `identificadorDispositivo`, `aplicacion`, etc. (los mismos que usa hoy para Terpel/7011).

3. **Enviar a 7011**
   - **POST** `https://<host>:7011/guru.facturacion/EnviarDatosMovimientoDian`
   - Body: JSON con:
     - `venta`: al menos `identificadorMovimiento`; el resto como lo tenga ya el flujo Java.
     - `cliente`: objeto completo (respuesta de consultarCliente o `atributos.factura_electronica`).
     - `identificadorMovimiento`: id del movimiento.
   - Headers: mismos que en consultarCliente.

4. **Respuesta 7011**
   - En la respuesta viene el CUFE (y lo que guarde vuestra capa Java/BD).
   - Después de éxito, disparar la **impresión automática** como hoy (ya con CUFE disponible).

### Estructura de payload para EnviarDatosMovimientoDian

Debe ser la misma que usa la UI Java / FacturacionElectronicaUtils. Mínimo:

```json
{
  "venta": {
    "identificadorMovimiento": 123
  },
  "cliente": {
    "numeroDocumento": "222222222222",
    "tipoDocumento": 13,
    "nombreComercial": "CONSUMIDOR FINAL",
    "nombreRazonSocial": "CONSUMIDOR FINAL",
    "direccion": "...",
    "correoElectronico": "...",
    "ciudad": "...",
    "departamento": "..."
  },
  "identificadorMovimiento": 123
}
```

---

## Resumen

| Qué hace | Opción A (API Flutter) | Opción B (LazoExpress) |
|----------|------------------------|-------------------------|
| Quién llama al 7011 | api-consultas-flutter | LazoExpress (Java) |
| URL del 7011 | Desde URL_GLOBAL en la API | LazoExpress lee `wacher_parametros` (HOST_SERVER) |
| Impresión automática | La puede hacer la API (`imprimir_despues: true`) o LazoExpress | Siempre LazoExpress después del 7011 |

Recomendación: **Opción A** para no duplicar lógica de URL_GLOBAL ni del cliente 7011; LazoExpress solo hace una llamada HTTP a la API de Flutter.

---

## Cuando LazoExpress esté listo: contrato del backend

El backend (api-consultas-flutter) ya está listo. Solo falta que LazoExpress llame en el punto de **validación de impresión automática**.

| Concepto | Valor |
|----------|--------|
| **URL** | `POST http://<host>:8020/ventas/enviar-fe-pump` (host donde corre api-consultas-flutter) |
| **Content-Type** | `application/json` |

**Body (ejemplo):**
```json
{
  "payload_fe": {
    "venta": { "identificadorMovimiento": 412 },
    "cliente": {
      "numeroDocumento": "222222222222",
      "tipoDocumento": 13,
      "nombreComercial": "CONSUMIDOR FINAL",
      "nombreRazonSocial": "CONSUMIDOR FINAL",
      "direccion": "CL 94 51 B 43",
      "correoElectronico": "emision.electronicamasser@masser.com.co",
      "ciudad": "BARRANQUILLA",
      "departamento": "ATLÁNTICO"
    },
    "identificadorMovimiento": 412
  },
  "identificador_movimiento": 412,
  "imprimir_despues": true,
  "tipo_reporte": "FACTURA-ELECTRONICA"
}
```

**Respuesta 200:** `{ "ok": true, "response_7011": { ... }, "impresion_enviada": true }` → El backend ya envió al 7011 y disparó la impresión; LazoExpress no debe imprimir de nuevo.

**Respuesta 4xx/5xx:** Error (ej. 502 si falla el 7011). El body incluye `detail` con el mensaje. LazoExpress puede mostrar error o no imprimir.

**Configuración del backend para impresión:** Si la impresión la hace el servicio en puerto 8001, en el entorno del backend poner `PRINT_SERVICE_URL=http://127.0.0.1:8001`. Así, cuando LazoExpress llame con `imprimir_despues: true`, el backend disparará `POST http://127.0.0.1:8001/print-ticket/sales` con `movement_id`, `flow_type`, `report_type`.

Cuando tengas abierto el proyecto LazoExpress en Cursor, se puede implementar juntos el punto exacto (clase/método) donde hacer esta llamada.
