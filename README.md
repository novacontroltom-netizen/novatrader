# NOVA RENDER WATCHER V2 — WEBSOCKET MODE

Objetivo: reducir drásticamente el uso de REST de Binance.

## Diseño
- `!miniTicker@arr` por WebSocket: precios + volumen 24h de todo el mercado.
- `@kline_1m` por WebSocket: velas de los 8 candidatos.
- REST sólo para bootstrap de 40 velas de un candidato nuevo.
- candidatos se recalculan cada 5 minutos.
- decisiones paper cada 50 segundos.
- si Binance responde 418/429, el REST entra en backoff automáticamente.
- BUY/SELL siguen ocurriendo en Cloudflare Worker V1.7 + D1.

## Variables Render
NOVA_WORKER_URL=https://nova-trader-engine.cos-tortugasopen.workers.dev
NOVA_WATCHER_TOKEN=<MISMO WATCHER_TOKEN DE CLOUDFLARE>

## Endpoints
/status
/market
/binance-test
/btc

## Nota Render Free
El loop funciona mientras la instancia está activa. El plan Free puede suspenderla por falta de tráfico externo.
