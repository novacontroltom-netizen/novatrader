import express from "express";

const app = express();
const PORT = process.env.PORT || 10000;
const BINANCE = "https://api-gcp.binance.com";

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "NOVA RENDER BINANCE TEST",
    version: "1.0.0",
    routes: ["/health", "/binance-test", "/btc"]
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "NOVA RENDER BINANCE TEST",
    time: new Date().toISOString()
  });
});

app.get("/binance-test", async (_req, res) => {
  const started = Date.now();
  try {
    const r = await fetch(BINANCE + "/api/v3/ping", {
      headers: {
        "Accept": "application/json",
        "User-Agent": "NOVA-RENDER-TEST/1.0"
      }
    });
    const body = await r.text();
    res.status(r.status).json({
      ok: r.ok,
      status: r.status,
      ms: Date.now() - started,
      upstream: BINANCE,
      body
    });
  } catch (e) {
    res.status(502).json({
      ok: false,
      error: String(e?.message || e),
      ms: Date.now() - started
    });
  }
});

app.get("/btc", async (_req, res) => {
  try {
    const r = await fetch(BINANCE + "/api/v3/ticker/price?symbol=BTCUSDT", {
      headers: {
        "Accept": "application/json",
        "User-Agent": "NOVA-RENDER-TEST/1.0"
      }
    });
    const body = await r.text();
    res.status(r.status);
    res.set("content-type", "application/json; charset=utf-8");
    res.send(body);
  } catch (e) {
    res.status(502).json({
      ok: false,
      error: String(e?.message || e)
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`NOVA Render Binance Test running on port ${PORT}`);
});
