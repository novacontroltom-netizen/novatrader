import express from "express";
import WebSocket from "ws";

const app = express();
app.disable("x-powered-by");

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

const REST = "https://api-gcp.binance.com";
const WS_BASE = "wss://stream.binance.com:9443";
const DECISION_MS = 50_000;
const CANDIDATE_REFRESH_MS = 5 * 60_000;
const DEEP_LIMIT = 8;
const MIN_VOL = 5_000_000;
const ENTRY_SCORE = 78;
const MAX_CANDLE_AGE_MS = 3 * 60 * 1000;

let running = false;
let lastCycle = null;
let lastMarket = [];
let candidateSymbols = [];
let lastCandidateRefresh = 0;
let restBlockedUntil = 0;

const mini = new Map();         // symbol -> miniTicker snapshot
const candles = new Map();     // symbol -> closed/current candles
let miniWs = null;
let klineWs = null;
let miniWsConnected = false;
let klineWsConnected = false;
let reconnectMiniTimer = null;
let reconnectKlineTimer = null;

const clamp=(n,a,b)=>Math.min(b,Math.max(a,n));
const avg=a=>a.length?a.reduce((x,y)=>x+(Number(y)||0),0)/a.length:0;
const pct=(a,b)=>b?((a/b)-1)*100:0;
const round=(n,d=3)=>Math.round(Number(n)*10**d)/10**d;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

function excluded(symbol){
  const base=symbol.slice(0,-4);
  return new Set(["USDC","FDUSD","TUSD","USDP","DAI","EUR","TRY","BRL","BUSD","AEUR","EURI"]).has(base)
    || /(UP|DOWN|BULL|BEAR)$/.test(base);
}

async function worker(path,opt={}){
  if(!WATCHER_TOKEN) throw new Error("Falta NOVA_WATCHER_TOKEN en Render");
  const res=await fetch(WORKER_URL+path,{
    ...opt,
    headers:{
      "Accept":"application/json",
      "Content-Type":"application/json",
      "x-nova-watcher-token":WATCHER_TOKEN,
      ...(opt.headers||{})
    }
  });
  const text=await res.text();
  if(!res.ok) throw new Error(`Worker HTTP ${res.status}: ${text.slice(0,350)}`);
  return JSON.parse(text);
}

function noteRestBan(status,text,res){
  if(status!==418 && status!==429) return;
  const retry=Number(res?.headers?.get("retry-after")||0);
  let until=retry?Date.now()+retry*1000:Date.now()+10*60*1000;
  const m=String(text).match(/banned until\s+(\d{10,16})/i);
  if(m){
    const n=Number(m[1]);
    if(Number.isFinite(n)) until=n>1e12?n:n*1000;
  }
  restBlockedUntil=Math.max(restBlockedUntil,until);
  console.error(`REST pausado hasta ${new Date(restBlockedUntil).toISOString()} por HTTP ${status}`);
}

async function restGet(path){
  if(Date.now()<restBlockedUntil){
    throw new Error(`REST_BACKOFF hasta ${new Date(restBlockedUntil).toISOString()}`);
  }
  const res=await fetch(REST+path,{
    headers:{"Accept":"application/json","User-Agent":"NOVA-RENDER-WS/2.0"}
  });
  const text=await res.text();
  if(!res.ok){
    noteRestBan(res.status,text,res);
    throw new Error(`Binance REST HTTP ${res.status}: ${text.slice(0,260)}`);
  }
  return JSON.parse(text);
}

function connectMiniTicker(){
  clearTimeout(reconnectMiniTimer);
  try{ if(miniWs) miniWs.terminate(); }catch{}
  miniWs=new WebSocket(`${WS_BASE}/ws/!miniTicker@arr`);

  miniWs.on("open",()=>{
    miniWsConnected=true;
    console.log("WS miniTicker conectado");
  });

  miniWs.on("message",buf=>{
    try{
      const arr=JSON.parse(buf.toString());
      if(!Array.isArray(arr)) return;
      const now=Date.now();
      for(const t of arr){
        if(!t?.s) continue;
        mini.set(t.s,{
          symbol:t.s,
          price:Number(t.c),
          open:Number(t.o),
          high:Number(t.h),
          low:Number(t.l),
          baseVolume:Number(t.v),
          quoteVolume:Number(t.q),
          updatedAt:now
        });
      }
    }catch(e){
      console.error("miniTicker parse:",e.message);
    }
  });

  miniWs.on("close",()=>{
    miniWsConnected=false;
    console.error("WS miniTicker cerrado; reconecto en 5s");
    reconnectMiniTimer=setTimeout(connectMiniTicker,5000);
  });
  miniWs.on("error",e=>console.error("WS miniTicker error:",e.message));
}

function connectKlines(){
  clearTimeout(reconnectKlineTimer);
  try{ if(klineWs) klineWs.terminate(); }catch{}
  klineWs=new WebSocket(`${WS_BASE}/ws`);

  klineWs.on("open",()=>{
    klineWsConnected=true;
    console.log("WS klines conectado");
    syncKlineSubscriptions([],candidateSymbols);
  });

  klineWs.on("message",buf=>{
    try{
      const msg=JSON.parse(buf.toString());
      if(!msg?.e || msg.e!=="kline" || !msg.k) return;
      const k=msg.k;
      const symbol=msg.s;
      const candle={
        open:Number(k.o),high:Number(k.h),low:Number(k.l),close:Number(k.c),
        quoteVolume:Number(k.q),openTime:Number(k.t),closeTime:Number(k.T),closed:!!k.x
      };
      const arr=candles.get(symbol)||[];
      const i=arr.findIndex(x=>x.openTime===candle.openTime);
      if(i>=0) arr[i]=candle;
      else arr.push(candle);
      arr.sort((a,b)=>a.openTime-b.openTime);
      while(arr.length>45) arr.shift();
      candles.set(symbol,arr);
    }catch(e){
      // subscription ACKs have no e, ignore
    }
  });

  klineWs.on("close",()=>{
    klineWsConnected=false;
    console.error("WS klines cerrado; reconecto en 5s");
    reconnectKlineTimer=setTimeout(connectKlines,5000);
  });
  klineWs.on("error",e=>console.error("WS klines error:",e.message));
}

function sendWs(obj){
  if(klineWs?.readyState===WebSocket.OPEN){
    klineWs.send(JSON.stringify(obj));
  }
}

function syncKlineSubscriptions(oldList,newList){
  if(klineWs?.readyState!==WebSocket.OPEN) return;
  const oldSet=new Set(oldList);
  const newSet=new Set(newList);
  const unsub=[...oldSet].filter(s=>!newSet.has(s)).map(s=>`${s.toLowerCase()}@kline_1m`);
  const sub=[...newSet].filter(s=>!oldSet.has(s)).map(s=>`${s.toLowerCase()}@kline_1m`);
  if(unsub.length) sendWs({method:"UNSUBSCRIBE",params:unsub,id:Date.now()});
  if(sub.length) sendWs({method:"SUBSCRIBE",params:sub,id:Date.now()+1});
}

async function bootstrapCandles(symbol){
  if((candles.get(symbol)||[]).length>=25) return;
  const data=await restGet(`/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=1m&limit=40`);
  const arr=data.map(k=>({
    open:Number(k[1]),high:Number(k[2]),low:Number(k[3]),close:Number(k[4]),
    quoteVolume:Number(k[7]),openTime:Number(k[0]),closeTime:Number(k[6]),closed:Number(k[6])<Date.now()
  }));
  candles.set(symbol,arr);
}

function chooseCandidates(){
  const universe=[];
  for(const t of mini.values()){
    if(!t.symbol.endsWith("USDT") || excluded(t.symbol) || t.quoteVolume<MIN_VOL || !(t.price>0) || !(t.open>0)) continue;
    const change24h=((t.price/t.open)-1)*100;
    const pre=Math.min(35,Math.abs(change24h)*3.5)+
      Math.min(25,Math.max(0,Math.log10(Math.max(t.quoteVolume,1))-6)*8);
    universe.push({...t,change24h,pre});
  }
  return universe.sort((a,b)=>b.pre-a.pre).slice(0,DEEP_LIMIT).map(x=>x.symbol);
}

async function refreshCandidates(force=false){
  if(!force && Date.now()-lastCandidateRefresh<CANDIDATE_REFRESH_MS) return;
  if(mini.size<50) return; // wait for websocket universe

  const next=chooseCandidates();
  if(!next.length) return;
  const old=[...candidateSymbols];
  candidateSymbols=next;
  lastCandidateRefresh=Date.now();
  syncKlineSubscriptions(old,next);

  // Bootstrap only the selected symbols. This is the only routine REST usage.
  for(const s of next){
    if((candles.get(s)||[]).length>=25) continue;
    try{
      await bootstrapCandles(s);
      await sleep(220); // gentle spacing
    }catch(e){
      console.error(`Bootstrap ${s}:`,e.message);
      if(Date.now()<restBlockedUntil) break;
    }
  }
  console.log("Candidatos:",candidateSymbols.join(", "));
}

function analyze(symbol){
  const t=mini.get(symbol);
  const arr=(candles.get(symbol)||[]).slice().sort((a,b)=>a.openTime-b.openTime);
  if(!t || arr.length<25) return null;

  const last=arr.at(-1);
  const now=Date.now();
  const dataAge=Math.min(now-(t.updatedAt||0),now-(last.closeTime||0));
  if(dataAge>MAX_CANDLE_AGE_MS) return {
    symbol,price:t.price,state:"DATA_STALE",breakoutScore:0,
    move1m:0,move5m:0,move15m:0,change24h:0,quoteVolume24h:t.quoteVolume,
    volumeRatio:0,previousHigh20:null,breakoutPct:0,range20Pct:0,acceleration:0,
    chaseRisk:true,dataStale:true,candleAgeSeconds:Math.round(dataAge/1000),
    reasons:["datos viejos"],candleTime:new Date(last.closeTime).toISOString()
  };

  // Use latest market price plus recent minute candles.
  const price=t.price;
  const closes=arr.map(x=>x.close);
  const ref1=closes.at(-2) ?? closes.at(-1);
  const ref5=closes.at(-6) ?? closes[0];
  const ref15=closes.at(-16) ?? closes[0];
  const m1=pct(price,ref1), m5=pct(price,ref5), m15=pct(price,ref15);

  const prev20=arr.slice(-21,-1);
  const high20=Math.max(...prev20.map(x=>x.high));
  const low20=Math.min(...prev20.map(x=>x.low));

  const recentVol=avg(arr.slice(-3).map(x=>x.quoteVolume));
  const baseVol=avg(arr.slice(-18,-3).map(x=>x.quoteVolume));
  const vr=baseVol?recentVol/baseVol:1;
  const breakoutPct=((price/high20)-1)*100;
  const prior5=pct(ref5,closes.at(-11)??closes[0]);
  const accel=m5-prior5;
  const change24h=t.open?((price/t.open)-1)*100:0;

  let score=0;
  score+=clamp(m1*16,0,18);
  score+=clamp(m5*8,0,25);
  score+=clamp(m15*2.5,0,10);
  score+=clamp((vr-1)*14,0,25);
  if(price>high20) score+=16+clamp(breakoutPct*4,0,8);
  else if(breakoutPct>-0.35) score+=8;
  score+=clamp(accel*6,0,10);
  if(t.quoteVolume>=100_000_000) score+=5;
  else if(t.quoteVolume>=25_000_000) score+=3;

  let chaseRisk=false;
  if(m5>12 || m15>25){chaseRisk=true;score-=18;}
  score=Math.round(clamp(score,0,100));

  const state=
    score>=ENTRY_SCORE && m5>0 && vr>=1.25 && !chaseRisk ? "BREAKOUT" :
    score>=60 ? "WATCH" :
    m5<-1.2 ? "FALLING" : "CALM";

  const reasons=[];
  if(m1>=0.5) reasons.push(`subida rápida 1m +${round(m1,2)}%`);
  if(m5>=1) reasons.push(`impulso 5m +${round(m5,2)}%`);
  if(vr>=1.5) reasons.push(`volumen x${vr.toFixed(2)}`);
  if(price>high20) reasons.push("rompió máximo de 20 minutos");
  else if(breakoutPct>-0.35) reasons.push("cerca del máximo de 20 minutos");
  if(accel>=0.5) reasons.push("aceleración positiva");
  if(chaseRisk) reasons.push("riesgo de perseguir subida vertical");

  return {
    symbol,price,state,breakoutScore:score,
    move1m:round(m1),move5m:round(m5),move15m:round(m15),
    change24h:round(change24h),quoteVolume24h:Math.round(t.quoteVolume),
    volumeRatio:round(vr),previousHigh20:high20,breakoutPct:round(breakoutPct),
    range20Pct:round(((high20/low20)-1)*100),acceleration:round(accel),
    chaseRisk,dataStale:false,candleAgeSeconds:Math.round((Date.now()-t.updatedAt)/1000),
    reasons,candleTime:new Date(last.closeTime).toISOString()
  };
}

async function runCycle(){
  if(running) return;
  running=true;
  const started=Date.now();
  try{
    await refreshCandidates(false);
    const state=await worker("/watcher/state");

    const prices={};
    const openSymbols=[...new Set((state.openPositions||[]).map(p=>p.symbol))];
    for(const s of openSymbols){
      const t=mini.get(s);
      if(t?.price>0) prices[s]=t.price;
    }

    const signals=candidateSymbols.map(analyze).filter(Boolean).sort((a,b)=>b.breakoutScore-a.breakoutScore);
    for(const s of signals) prices[s.symbol]=s.price;

    const result=await worker("/watcher/cycle",{
      method:"POST",
      body:JSON.stringify({
        source:"RENDER_WS_50S",
        generatedAt:Date.now(),
        prices,signals
      })
    });

    lastMarket=signals;
    lastCycle={
      ok:true,startedAt:new Date(started).toISOString(),finishedAt:new Date().toISOString(),
      durationMs:Date.now()-started,openPositions:openSymbols.length,analyzed:signals.length,
      entries:result.entries||[],exits:result.exits||[],
      websocket:{mini:miniWsConnected,klines:klineWsConnected},
      restBlockedUntil:restBlockedUntil?new Date(restBlockedUntil).toISOString():null,
      top:signals.slice(0,8).map(s=>({
        symbol:s.symbol,price:s.price,score:s.breakoutScore,state:s.state,
        move1m:s.move1m,move5m:s.move5m,move15m:s.move15m,
        volumeRatio:s.volumeRatio,reasons:s.reasons
      }))
    };
    console.log(`[${new Date().toISOString()}] OK ${lastCycle.durationMs}ms | WS=${miniWsConnected&&klineWsConnected} | abiertas=${openSymbols.length} | analizadas=${signals.length} | BUY=${lastCycle.entries.length} | SELL=${lastCycle.exits.length}`);
  }catch(e){
    lastCycle={
      ok:false,startedAt:new Date(started).toISOString(),finishedAt:new Date().toISOString(),
      durationMs:Date.now()-started,error:String(e?.message||e),
      websocket:{mini:miniWsConnected,klines:klineWsConnected},
      restBlockedUntil:restBlockedUntil?new Date(restBlockedUntil).toISOString():null
    };
    console.error("NOVA WATCHER ERROR:",e?.message||e);
  }finally{running=false}
}

// HTTP
app.get("/",(_req,res)=>res.json({
  ok:true,service:"NOVA RENDER WATCHER",version:"2.0.0",mode:"WEBSOCKET",
  realTrading:false,intervalSeconds:50,tokenConfigured:!!WATCHER_TOKEN,
  websocket:{mini:miniWsConnected,klines:klineWsConnected},
  candidates:candidateSymbols,lastCycle,
  routes:["/health","/status","/market","/binance-test","/btc","/run-now"]
}));

app.get("/health",(_req,res)=>res.json({
  ok:true,service:"NOVA RENDER WATCHER",version:"2.0.0",mode:"WEBSOCKET",
  websocket:{mini:miniWsConnected,klines:klineWsConnected},
  miniSymbols:mini.size,candidates:candidateSymbols.length,
  restBlockedUntil:restBlockedUntil?new Date(restBlockedUntil).toISOString():null,
  time:new Date().toISOString()
}));

app.get("/status",(_req,res)=>res.json({
  ok:true,running,intervalSeconds:50,mode:"WEBSOCKET",
  websocket:{mini:miniWsConnected,klines:klineWsConnected},
  miniSymbols:mini.size,candidates:candidateSymbols,lastCycle
}));

app.get("/market",(_req,res)=>res.json({
  ok:true,source:"RENDER_WS_50S",generatedAt:lastCycle?.finishedAt||null,
  websocket:{mini:miniWsConnected,klines:klineWsConnected},
  top:lastMarket.slice(0,8).map(s=>({
    symbol:s.symbol,price:s.price,score:s.breakoutScore,state:s.state,
    move1m:s.move1m,move5m:s.move5m,move15m:s.move15m,
    volumeRatio:s.volumeRatio,reasons:s.reasons
  }))
}));

app.get("/binance-test",async(_req,res)=>{
  res.json({
    ok:miniWsConnected,
    mode:"WEBSOCKET",
    miniTickerConnected:miniWsConnected,
    klineConnected:klineWsConnected,
    symbolsInMemory:mini.size,
    restBlockedUntil:restBlockedUntil?new Date(restBlockedUntil).toISOString():null
  });
});

app.get("/btc",(_req,res)=>{
  const t=mini.get("BTCUSDT");
  if(!t) return res.status(503).json({ok:false,error:"BTCUSDT todavía no llegó por WebSocket"});
  res.json({symbol:"BTCUSDT",price:String(t.price),source:"WEBSOCKET"});
});

app.post("/run-now",async(_req,res)=>{
  if(running) return res.status(409).json({ok:false,error:"Ya hay un ciclo en ejecución"});
  await runCycle();
  res.json({ok:true,lastCycle});
});

app.listen(PORT,"0.0.0.0",()=>{
  console.log(`NOVA RENDER WATCHER V2 WebSocket en puerto ${PORT}`);
  console.log("Paper trading ONLY.");
  connectMiniTicker();
  connectKlines();

  setTimeout(async()=>{
    try{await refreshCandidates(true)}catch(e){console.error("Inicial candidatos:",e.message)}
    runCycle();
  },8000);

  setInterval(runCycle,DECISION_MS);
  setInterval(()=>refreshCandidates(false).catch(e=>console.error("Refresh candidatos:",e.message)),60_000);
});
