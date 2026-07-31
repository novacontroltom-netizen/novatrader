import express from "express";

const app = express();
app.disable("x-powered-by");

// CORS para que el dashboard alojado en Cloudflare Pages pueda leer
// /status, /health, /binance-test y /btc directamente desde Render.
app.use((req,res,next)=>{
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  res.setHeader("Cache-Control","no-store");
  if(req.method==="OPTIONS") return res.sendStatus(204);
  next();
});

const PORT = process.env.PORT || 10000;
const WORKER_URL = process.env.NOVA_WORKER_URL || "https://nova-trader-engine.cos-tortugasopen.workers.dev";
const WATCHER_TOKEN = process.env.NOVA_WATCHER_TOKEN || "";
const BINANCE = "https://api-gcp.binance.com";

const INTERVAL_MS = 50_000;
const DEEP_LIMIT = 8;
const MIN_VOL = 5_000_000;
const ENTRY_SCORE = 78;
const MAX_CANDLE_AGE_MS = 3 * 60 * 1000;

let running = false;
let lastCycle = null;

const clamp = (n,a,b) => Math.min(b,Math.max(a,n));
const avg = a => a.length ? a.reduce((x,y)=>x+(Number(y)||0),0)/a.length : 0;
const pct = (a,b) => b ? ((a/b)-1)*100 : 0;
const round = (n,d=3) => Math.round(Number(n)*10**d)/10**d;

function excluded(symbol){
  const base=symbol.slice(0,-4);
  return new Set([
    "USDC","FDUSD","TUSD","USDP","DAI","EUR","TRY","BRL",
    "BUSD","AEUR","EURI"
  ]).has(base) || /(UP|DOWN|BULL|BEAR)$/.test(base);
}

async function bget(path){
  const res = await fetch(BINANCE + path, {
    headers: {
      "Accept":"application/json",
      "User-Agent":"NOVA-RENDER-WATCHER/1.2"
    }
  });

  if(!res.ok){
    const text=await res.text();
    throw new Error(`Binance HTTP ${res.status}: ${text.slice(0,220)}`);
  }

  return res.json();
}

async function worker(path,opt={}){
  if(!WATCHER_TOKEN) throw new Error("Falta NOVA_WATCHER_TOKEN en Render");

  const res = await fetch(WORKER_URL + path, {
    ...opt,
    headers:{
      "Accept":"application/json",
      "Content-Type":"application/json",
      "x-nova-watcher-token":WATCHER_TOKEN,
      ...(opt.headers||{})
    }
  });

  const text=await res.text();

  if(!res.ok){
    throw new Error(`Worker HTTP ${res.status}: ${text.slice(0,350)}`);
  }

  return JSON.parse(text);
}

async function analyzeSymbol(symbol,hint=null){
  const [ticker,klines]=await Promise.all([
    hint ? Promise.resolve(hint) : bget(`/api/v3/ticker/24hr?symbol=${encodeURIComponent(symbol)}`),
    bget(`/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=1m&limit=40`)
  ]);

  const t=hint ? hint : {
    symbol,
    price:Number(ticker.lastPrice),
    change24h:Number(ticker.priceChangePercent),
    quoteVolume24h:Number(ticker.quoteVolume)
  };

  const c=klines.map(k=>({
    high:Number(k[2]),
    low:Number(k[3]),
    close:Number(k[4]),
    quoteVolume:Number(k[7]),
    closeTime:Number(k[6])
  }));

  if(c.length<25) throw new Error(`Velas insuficientes: ${symbol}`);

  const last=c.at(-1);
  const age=Date.now()-last.closeTime;

  if(age>MAX_CANDLE_AGE_MS){
    return {
      symbol,
      price:last.close,
      state:"DATA_STALE",
      breakoutScore:0,
      move1m:0,
      move5m:0,
      move15m:0,
      change24h:round(t.change24h),
      quoteVolume24h:Math.round(t.quoteVolume24h),
      volumeRatio:0,
      previousHigh20:null,
      breakoutPct:0,
      range20Pct:0,
      acceleration:0,
      chaseRisk:true,
      dataStale:true,
      candleAgeSeconds:Math.round(age/1000),
      reasons:[`datos viejos: última vela hace ${Math.round(age/60000)} min`],
      candleTime:new Date(last.closeTime).toISOString()
    };
  }

  const prev20=c.slice(-21,-1);
  const high20=Math.max(...prev20.map(x=>x.high));
  const low20=Math.min(...prev20.map(x=>x.low));

  const m1=pct(last.close,c.at(-2).close);
  const m5=pct(last.close,c.at(-6).close);
  const m15=pct(last.close,c.at(-16).close);

  const recentVol=avg(c.slice(-3).map(x=>x.quoteVolume));
  const baseVol=avg(c.slice(-18,-3).map(x=>x.quoteVolume));
  const vr=baseVol ? recentVol/baseVol : 1;

  const breakoutPct=((last.close/high20)-1)*100;
  const prior5=pct(c.at(-6).close,c.at(-11).close);
  const accel=m5-prior5;

  let score=0;
  score+=clamp(m1*16,0,18);
  score+=clamp(m5*8,0,25);
  score+=clamp(m15*2.5,0,10);
  score+=clamp((vr-1)*14,0,25);

  if(last.close>high20) score+=16+clamp(breakoutPct*4,0,8);
  else if(breakoutPct>-0.35) score+=8;

  score+=clamp(accel*6,0,10);

  if(t.quoteVolume24h>=100_000_000) score+=5;
  else if(t.quoteVolume24h>=25_000_000) score+=3;

  let chaseRisk=false;
  if(m5>12 || m15>25){
    chaseRisk=true;
    score-=18;
  }

  score=Math.round(clamp(score,0,100));

  const state=
    score>=ENTRY_SCORE && m5>0 && vr>=1.25 && !chaseRisk ? "BREAKOUT" :
    score>=60 ? "WATCH" :
    m5<-1.2 ? "FALLING" :
    "CALM";

  const reasons=[];
  if(m1>=0.5) reasons.push(`subida rápida 1m +${round(m1,2)}%`);
  if(m5>=1) reasons.push(`impulso 5m +${round(m5,2)}%`);
  if(vr>=1.5) reasons.push(`volumen x${vr.toFixed(2)}`);
  if(last.close>high20) reasons.push("rompió máximo de 20 minutos");
  else if(breakoutPct>-0.35) reasons.push("cerca del máximo de 20 minutos");
  if(accel>=0.5) reasons.push("aceleración positiva");
  if(chaseRisk) reasons.push("riesgo de perseguir subida vertical");

  return {
    symbol,
    price:last.close,
    state,
    breakoutScore:score,
    move1m:round(m1),
    move5m:round(m5),
    move15m:round(m15),
    change24h:round(t.change24h),
    quoteVolume24h:Math.round(t.quoteVolume24h),
    volumeRatio:round(vr),
    previousHigh20:high20,
    breakoutPct:round(breakoutPct),
    range20Pct:round(((high20/low20)-1)*100),
    acceleration:round(accel),
    chaseRisk,
    dataStale:false,
    candleAgeSeconds:Math.round(age/1000),
    reasons,
    candleTime:new Date(last.closeTime).toISOString()
  };
}

async function runCycle(){
  if(running){
    console.log("Ciclo omitido: el anterior todavía está ejecutándose.");
    return;
  }

  running=true;
  const started=Date.now();

  try{
    const state=await worker("/watcher/state");

    // PROTECCIÓN PRIMERO: siempre obtenemos precio de todas las posiciones abiertas.
    const openSymbols=[...new Set((state.openPositions||[]).map(p=>p.symbol))];
    const prices={};

    await Promise.all(openSymbols.map(async symbol=>{
      const x=await bget(`/api/v3/ticker/price?symbol=${encodeURIComponent(symbol)}`);
      prices[symbol]=Number(x.price);
    }));

    // Luego buscamos nuevas rupturas.
    const tickers=await bget("/api/v3/ticker/24hr");

    const universe=tickers
      .filter(t=>t.symbol.endsWith("USDT"))
      .filter(t=>Number(t.quoteVolume)>=MIN_VOL)
      .filter(t=>!excluded(t.symbol))
      .map(t=>({
        symbol:t.symbol,
        price:Number(t.lastPrice),
        change24h:Number(t.priceChangePercent),
        quoteVolume24h:Number(t.quoteVolume)
      }));

    const candidates=universe
      .map(t=>({
        ...t,
        pre:
          Math.min(35,Math.abs(t.change24h)*3.5)+
          Math.min(25,Math.max(0,Math.log10(Math.max(t.quoteVolume24h,1))-6)*8)
      }))
      .sort((a,b)=>b.pre-a.pre)
      .slice(0,DEEP_LIMIT);

    const settled=await Promise.allSettled(
      candidates.map(t=>analyzeSymbol(t.symbol,t))
    );

    const signals=settled
      .filter(x=>x.status==="fulfilled")
      .map(x=>x.value)
      .sort((a,b)=>b.breakoutScore-a.breakoutScore);

    for(const s of signals){
      prices[s.symbol]=Number(s.price);
    }

    const result=await worker("/watcher/cycle",{
      method:"POST",
      body:JSON.stringify({
        source:"RENDER_50S",
        generatedAt:Date.now(),
        prices,
        signals
      })
    });

    const durationMs=Date.now()-started;

    lastCycle={
      ok:true,
      startedAt:new Date(started).toISOString(),
      finishedAt:new Date().toISOString(),
      durationMs,
      openPositions:openSymbols.length,
      analyzed:signals.length,
      entries:result.entries||[],
      exits:result.exits||[],
      top:signals.slice(0,8).map(s=>({
        symbol:s.symbol,
        price:s.price,
        score:s.breakoutScore,
        state:s.state,
        move1m:s.move1m,
        move5m:s.move5m,
        move15m:s.move15m,
        volumeRatio:s.volumeRatio,
        reasons:s.reasons||[]
      }))
    };

    console.log(
      `[${new Date().toISOString()}] OK ${durationMs}ms | abiertas=${openSymbols.length} | analizadas=${signals.length} | BUY=${lastCycle.entries.length} | SELL=${lastCycle.exits.length}`
    );

    for(const x of lastCycle.entries){
      console.log(`BUY ${x.bot} ${x.symbol} @ ${x.price}`);
    }

    for(const x of lastCycle.exits){
      console.log(`SELL ${x.bot} ${x.symbol} @ ${x.price} ${x.reason} P/L ${x.pnlPct}%`);
    }

  }catch(e){
    lastCycle={
      ok:false,
      startedAt:new Date(started).toISOString(),
      finishedAt:new Date().toISOString(),
      durationMs:Date.now()-started,
      error:String(e?.message||e)
    };

    console.error("NOVA WATCHER ERROR:",e?.message||e);

  }finally{
    running=false;
  }
}

// ---------------- HTTP ----------------

app.get("/", (_req,res)=>{
  res.json({
    ok:true,
    service:"NOVA RENDER WATCHER",
    version:"1.2.0",
    realTrading:false,
    intervalSeconds:INTERVAL_MS/1000,
    worker:WORKER_URL,
    binance:BINANCE,
    tokenConfigured:!!WATCHER_TOKEN,
    running,
    lastCycle,
    routes:["/health","/status","/market","/binance-test","/btc","/run-now"]
  });
});

app.get("/health",(_req,res)=>{
  res.json({
    ok:true,
    service:"NOVA RENDER WATCHER",
    version:"1.2.0",
    realTrading:false,
    intervalSeconds:50,
    tokenConfigured:!!WATCHER_TOKEN,
    running,
    time:new Date().toISOString()
  });
});

app.get("/status",(_req,res)=>{
  res.json({
    ok:true,
    running,
    intervalSeconds:50,
    lastCycle
  });
});

app.get("/market",(_req,res)=>{
  res.json({
    ok:true,
    source:"RENDER_50S",
    generatedAt:lastCycle?.finishedAt||null,
    running,
    top:Array.isArray(lastCycle?.top)?lastCycle.top:[]
  });
});

app.get("/binance-test",async(_req,res)=>{
  const started=Date.now();
  try{
    const r=await fetch(BINANCE+"/api/v3/ping",{
      headers:{"Accept":"application/json","User-Agent":"NOVA-RENDER-WATCHER/1.2"}
    });
    const body=await r.text();
    res.status(r.status).json({
      ok:r.ok,
      status:r.status,
      ms:Date.now()-started,
      upstream:BINANCE,
      body
    });
  }catch(e){
    res.status(502).json({ok:false,error:String(e?.message||e)});
  }
});

app.get("/btc",async(_req,res)=>{
  try{
    const x=await bget("/api/v3/ticker/price?symbol=BTCUSDT");
    res.json(x);
  }catch(e){
    res.status(502).json({ok:false,error:String(e?.message||e)});
  }
});

app.post("/run-now",async(_req,res)=>{
  if(running){
    return res.status(409).json({ok:false,error:"Ya hay un ciclo en ejecución"});
  }
  await runCycle();
  res.json({ok:true,lastCycle});
});

app.listen(PORT,"0.0.0.0",()=>{
  console.log(`NOVA RENDER WATCHER escuchando en puerto ${PORT}`);
  console.log(`Paper trading ONLY. Ciclo cada ${INTERVAL_MS/1000}s.`);

  // Primer ciclo unos segundos después del arranque.
  setTimeout(()=>{
    runCycle();
  },5000);

  // Mientras la instancia exista, NOVA trabaja cada 50 segundos.
  setInterval(()=>{
    runCycle();
  },INTERVAL_MS);
});
