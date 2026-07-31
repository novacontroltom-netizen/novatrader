# NOVA RENDER WATCHER V1

Watcher paper-trading para Render.

## IMPORTANTE
Este servicio NO envía órdenes a Binance.
Sólo lee datos públicos de Binance y manda señales al Worker Cloudflare V1.7.

## Render

Usar:

- Web Service
- Runtime: Node
- Build Command: `npm install`
- Start Command: `npm start`
- Free mientras se prueba

## Variables de entorno OBLIGATORIAS

### NOVA_WATCHER_TOKEN
Debe ser EXACTAMENTE el mismo valor que el Secret `WATCHER_TOKEN`
configurado en Cloudflare Worker.

### NOVA_WORKER_URL
Valor:

`https://nova-trader-engine.cos-tortugasopen.workers.dev`

## Funcionamiento

Mientras la instancia Render esté ejecutándose:

- Primer ciclo a los 5 segundos.
- Ciclo automático cada 50 segundos.
- Primero revisa posiciones abiertas.
- Luego analiza 8 candidatos Binance Spot USDT.
- Envía precios + señales a `/watcher/cycle`.
- El Worker decide BUY/SELL PAPER y guarda en D1.

## Rutas

- `/`
- `/health`
- `/status`
- `/binance-test`
- `/btc`
- `POST /run-now`

## Verificación

Render:

`https://TU-APP.onrender.com/status`

Cloudflare:

`https://nova-trader-engine.cos-tortugasopen.workers.dev/watcher/status`

El `source` esperado en Cloudflare es:

`RENDER_50S`

## Render Free

El loop de 50 segundos funciona mientras la instancia está despierta.
Render Free puede suspender el Web Service por inactividad externa.
El propio `setInterval` no debe considerarse garantía de ejecución 24/7.
