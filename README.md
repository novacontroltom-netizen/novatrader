# NOVA RENDER BINANCE TEST

Prueba mínima para verificar si Render puede acceder a Binance.

## Configuración en Render
- Service Type: Web Service
- Runtime: Node
- Build Command: npm install
- Start Command: npm start
- Instance Type: Free

## Endpoints
- /
- /health
- /binance-test
- /btc

### Resultado deseado
`/binance-test` debe devolver `status: 200`.

`/btc` debe devolver un JSON con `BTCUSDT` y el precio.

No contiene claves de Binance.
No realiza operaciones reales.
