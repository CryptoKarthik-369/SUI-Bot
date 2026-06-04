// ═══════════════════════════════════════════════════════════
//  SUI SMC Bot — Server v4.0
//  SINGLE COMMAND:  node server.js
//  → Starts HTTP server  (port 4000)
//  → Auto-starts Cloudflare tunnel  (free HTTPS public URL)
//  → Prints Webhook URL + Dashboard WS URL in terminal
//  → Dashboard auto-receives tunnel URL via WebSocket
// ═══════════════════════════════════════════════════════════
//
//  INSTALL (once):
//    npm install express ws cors dotenv
//
//  START:
//    node server.js
//
//  TRADINGVIEW:
//    Paste the printed WEBHOOK URL into:
//    TradingView → Alert → Notifications → Webhook URL ✓
//
// ═══════════════════════════════════════════════════════════

require("dotenv").config();
const express              = require("express");
const http                 = require("http");
const { WebSocketServer }  = require("ws");
const cors                 = require("cors");
const crypto               = require("crypto");
const { execSync, spawn }  = require("child_process");
const os                   = require("os");

// ── CONFIG ────────────────────────────────────────────────
const PORT       = parseInt(process.env.PORT)   || 4000;
const SECRET     = process.env.WEBHOOK_SECRET   || "sui_smc_2024";
const MAX_ALERTS = 200;

// ── APP ───────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: "/ws" });

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "50kb" }));
app.use(express.text({ limit: "50kb" })); // TV sometimes sends plain-text JSON

// ── STATE ─────────────────────────────────────────────────
let publicUrl  = null;
let webhookUrl = null;
let wsLiveUrl  = null;
let alerts     = [];

let state = {
  symbol    : "SUIUSDT",
  lastPrice : null,
  lastTime  : null,
  d1 : { bias:"—", obTop:null, obBot:null },
  h1 : { zone:"—", demTop:null, demBot:null, supTop:null, supBot:null },
  m5 : { signal:"WATCHING" },
  activeSignal : null,
  connection : { publicUrl:null, webhookUrl:null, wsUrl:null, tunnelActive:false },
  stats : { total:0, longs:0, shorts:0, watches:0, today:0, date:new Date().toDateString() }
};

// ── ENRICH ALERT ──────────────────────────────────────────
function enrich(raw) {
  if (typeof raw === "string") {
    try { raw = JSON.parse(raw); }
    catch { return { id:crypto.randomUUID(), at:new Date().toISOString(), action:"RAW", type:"TEXT", price:0, raw }; }
  }
  const p   = parseFloat(raw.price)  || 0;
  const sl  = parseFloat(raw.sl)     || 0;
  const tp1 = parseFloat(raw.tp1)    || 0;
  const tp2 = parseFloat(raw.tp2)    || 0;
  const risk = Math.abs(p - sl);
  return {
    id       : crypto.randomUUID(),
    at       : new Date().toISOString(),
    symbol   : raw.symbol            || "SUIUSDT",
    action   : (raw.action || "INFO").toUpperCase(),
    type     : (raw.type   || "ALERT").toUpperCase(),
    tf       : raw.tf                || "5M",
    price:p, sl, tp1, tp2,
    rr       : risk > 0 ? (Math.abs(tp1-p)/risk).toFixed(2) : "—",
    d1Bias   : raw["1d_bias"]                      || null,
    d1ObTop  : parseFloat(raw["1d_ob_top"])        || null,
    d1ObBot  : parseFloat(raw["1d_ob_bot"])        || null,
    h1Zone   : raw["1h_ob"]                        || null,
    h1DemTop : parseFloat(raw["1h_dem_top"])       || null,
    h1DemBot : parseFloat(raw["1h_dem_bot"])       || null,
    h1SupTop : parseFloat(raw["1h_sup_top"])       || null,
    h1SupBot : parseFloat(raw["1h_sup_bot"])       || null,
    m5Signal : raw["5m_signal"]                    || null,
    raw
  };
}

// ── UPDATE STATE ──────────────────────────────────────────
function updateState(a) {
  state.lastPrice = a.price;
  state.lastTime  = a.at;

  if (a.d1Bias)   state.d1.bias    = a.d1Bias;
  if (a.d1ObTop)  state.d1.obTop   = a.d1ObTop;
  if (a.d1ObBot)  state.d1.obBot   = a.d1ObBot;
  if (a.h1Zone)   state.h1.zone    = a.h1Zone;
  if (a.h1DemTop) state.h1.demTop  = a.h1DemTop;
  if (a.h1DemBot) state.h1.demBot  = a.h1DemBot;
  if (a.h1SupTop) state.h1.supTop  = a.h1SupTop;
  if (a.h1SupBot) state.h1.supBot  = a.h1SupBot;
  if (a.m5Signal) state.m5.signal  = a.m5Signal;

  const today = new Date().toDateString();
  if (state.stats.date !== today) { state.stats.today = 0; state.stats.date = today; }
  state.stats.total++;
  state.stats.today++;
  if (a.action === "LONG"  && a.type === "ENTRY") { state.stats.longs++;  state.activeSignal = { dir:"LONG",  entry:a.price, sl:a.sl, tp1:a.tp1, tp2:a.tp2, rr:a.rr, at:a.at }; }
  if (a.action === "SHORT" && a.type === "ENTRY") { state.stats.shorts++; state.activeSignal = { dir:"SHORT", entry:a.price, sl:a.sl, tp1:a.tp1, tp2:a.tp2, rr:a.rr, at:a.at }; }
  if (a.action.startsWith("WATCH")) state.stats.watches++;

  alerts.unshift(a);
  if (alerts.length > MAX_ALERTS) alerts.length = MAX_ALERTS;
}

// ── BROADCAST ─────────────────────────────────────────────
function broadcast(event, data) {
  const msg = JSON.stringify({ event, data, ts: Date.now() });
  wss.clients.forEach(c => c.readyState === 1 && c.send(msg));
}

// ── WEBSOCKET ─────────────────────────────────────────────
wss.on("connection", ws => {
  console.log(`[WS] client connected  (total: ${wss.clients.size})`);
  // Send full snapshot including tunnel URLs immediately
  ws.send(JSON.stringify({
    event : "SNAPSHOT",
    data  : { state, alerts: alerts.slice(0, 50), publicUrl, webhookUrl, wsLiveUrl },
    ts    : Date.now()
  }));
  ws.on("message", raw => {
    try {
      const m = JSON.parse(raw.toString());
      if (m.type === "PING") ws.send(JSON.stringify({ event:"PONG", ts:Date.now() }));
    } catch {}
  });
  ws.on("close", () => console.log(`[WS] client left  (total: ${wss.clients.size})`));
  ws.on("error", e => console.error("[WS]", e.message));
});

// ── HTTP ROUTES ───────────────────────────────────────────

// Root — full status (open in browser to verify)
app.get("/", (req, res) => res.json({
  ok          : true,
  status      : "SUI SMC Bot ONLINE ✅",
  port        : PORT,
  tunnel      : publicUrl ? "ACTIVE" : "NOT STARTED",
  publicUrl,
  webhookUrl,
  wsLiveUrl,
  clients     : wss.clients.size,
  alerts      : alerts.length,
  state
}));

// Quick status (used by dashboard "Test" button)
app.get("/status", (req, res) => res.json({
  ok        : true,
  tunnel    : !!publicUrl,
  publicUrl,
  webhookUrl,
  wsLiveUrl,
  port      : PORT
}));

app.get("/api/state",  (req, res) => res.json({ ok:true, state, ts:Date.now() }));
app.get("/api/alerts", (req, res) => {
  const n = Math.min(parseInt(req.query.limit) || 50, MAX_ALERTS);
  res.json({ ok:true, alerts: alerts.slice(0, n) });
});

// ── WEBHOOK  (TradingView → POST here) ────────────────────
app.post("/webhook", (req, res) => {
  const s = req.query.secret || req.headers["x-secret"] || "";
  if (SECRET && s !== SECRET) {
    console.warn("[WEBHOOK] ⛔ bad secret from", req.ip);
    return res.status(401).json({ error: "bad secret" });
  }
  try {
    const alert = enrich(req.body);
    updateState(alert);
    broadcast("ALERT",        alert);
    broadcast("STATE_UPDATE", { state, alert });
    const line = `[ALERT] ${alert.action}@${alert.price}  1D:${state.d1.bias}  1H:${state.h1.zone}  5M:${state.m5.signal}`;
    console.log(line);
    if (alert.d1ObBot)  console.log(`        1D OB : ${alert.d1ObBot}–${alert.d1ObTop}`);
    if (alert.h1DemBot) console.log(`        1H DEM: ${alert.h1DemBot}–${alert.h1DemTop}`);
    if (alert.h1SupBot) console.log(`        1H SUP: ${alert.h1SupBot}–${alert.h1SupTop}`);
    res.json({ ok:true, id:alert.id });
  } catch(e) {
    console.error("[WEBHOOK] error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// Test alert (fire from dashboard button)
app.post("/test", (req, res) => {
  const p = parseFloat(state.lastPrice) || 3.824;
  const risk = p * 0.013;
  const payload = {
    symbol:"SUIUSDT", action:"LONG", type:"ENTRY", tf:"5M",
    price : p.toFixed(4),
    sl    : (p - risk).toFixed(4),
    tp1   : (p + risk * 1.5).toFixed(4),
    tp2   : (p + risk * 3.0).toFixed(4),
    "1d_bias"    : "BULLISH",
    "1d_ob_top"  : (p * 0.975).toFixed(4),
    "1d_ob_bot"  : (p * 0.960).toFixed(4),
    "1h_ob"      : "DEMAND",
    "1h_dem_top" : (p * 1.003).toFixed(4),
    "1h_dem_bot" : (p * 0.995).toFixed(4),
    "1h_sup_top" : (p * 1.060).toFixed(4),
    "1h_sup_bot" : (p * 1.052).toFixed(4),
    "5m_signal"  : "CHoCH_BULL",
    time: new Date().toISOString()
  };
  const alert = enrich(payload);
  updateState(alert);
  broadcast("ALERT",        alert);
  broadcast("STATE_UPDATE", { state, alert });
  console.log("[TEST] fired test LONG alert");
  res.json({ ok:true, alert });
});

// Heartbeat to keep dashboards fresh
setInterval(() => {
  if (wss.clients.size > 0)
    broadcast("HEARTBEAT", { ts:Date.now(), clients:wss.clients.size, state, publicUrl });
}, 25_000);

// ── SERVER START ──────────────────────────────────────────
server.listen(PORT, () => {
  console.log("\n╔══════════════════════════════════════════╗");
  console.log(`║   SUI SMC Bot  ·  http://localhost:${PORT}   ║`);
  console.log("╚══════════════════════════════════════════╝");
  console.log("  Starting Cloudflare Tunnel automatically...\n");
  startTunnel();
});

// ── CLOUDFLARE TUNNEL (built-in, no account needed) ───────
function startTunnel() {
  const platform = os.platform();

  // ① Check if cloudflared is already installed
  let installed = false;
  try { execSync("cloudflared --version", { stdio:"pipe" }); installed = true; }
  catch {}

  // ② Auto-install if missing
  if (!installed) {
    console.log("  ⬇  cloudflared not found — installing automatically...\n");
    try {
      if (platform === "win32") {
        // Windows — download binary directly
        execSync(
          `powershell -Command "Invoke-WebRequest -Uri 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe' -OutFile '${__dirname}\\cloudflared.exe'"`,
          { stdio:"inherit" }
        );
        process.env.PATH = __dirname + ";" + process.env.PATH;
      } else if (platform === "darwin") {
        execSync("brew install cloudflare/cloudflare/cloudflared", { stdio:"inherit" });
      } else {
        // Linux
        execSync(
          "curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared",
          { stdio:"inherit" }
        );
      }
      installed = true;
      console.log("\n  ✅ cloudflared installed\n");
    } catch(e) {
      console.log("  ⚠ Could not auto-install cloudflared.");
      console.log("  Manual install → https://developers.cloudflare.com/cloudflared/install");
      console.log("  Then run:  node server.js  again\n");
      return;
    }
  }

  // ③ Spawn tunnel
  const cfBin = platform === "win32" ? `${__dirname}\\cloudflared.exe` : "cloudflared";
  const cf    = spawn(cfBin, ["tunnel", "--url", `http://localhost:${PORT}`], {
    stdio: ["pipe", "pipe", "pipe"]
  });

  const parseOutput = (chunk) => {
    const text  = chunk.toString();
    const match = text.match(/https:\/\/[a-z0-9\-]+\.trycloudflare\.com/i);
    if (match && !publicUrl) {
      publicUrl   = match[0];
      webhookUrl  = `${publicUrl}/webhook?secret=${SECRET}`;
      wsLiveUrl   = `${publicUrl.replace("https://","wss://")}/ws`;

      // Update state connection object
      state.connection = { publicUrl, webhookUrl, wsUrl:wsLiveUrl, tunnelActive:true };

      // ── Print the two URLs clearly ──────────────────────
      const LINE = "═".repeat(60);
      console.log(`\n  ${LINE}`);
      console.log(`\n  ✅ TUNNEL ACTIVE\n`);
      console.log(`  📋 STEP 1 — Paste this in TradingView Webhook URL field:`);
      console.log(`\n     ${webhookUrl}\n`);
      console.log(`  🖥  STEP 2 — Paste this in the Dashboard (top bar):`);
      console.log(`\n     ${wsLiveUrl}\n`);
      console.log(`  🔍 STEP 3 — Verify server online:`);
      console.log(`\n     ${publicUrl}/status\n`);
      console.log(`  ${LINE}\n`);

      // ── Push URLs to any connected dashboard clients ────
      broadcast("TUNNEL_URL", {
        publicUrl,
        webhookUrl,
        wsUrl : wsLiveUrl
      });
    }
  };

  cf.stdout.on("data", parseOutput);
  cf.stderr.on("data", parseOutput);

  cf.on("exit", code => {
    if (code !== 0) {
      publicUrl  = null; webhookUrl = null; wsLiveUrl = null;
      state.connection.tunnelActive = false;
      console.log("  ⚠ Tunnel closed — restarting in 5s...");
      setTimeout(startTunnel, 5000);
    }
  });

  process.on("exit",    () => cf.kill());
  process.on("SIGINT",  () => { cf.kill(); process.exit(0); });
  process.on("SIGTERM", () => { cf.kill(); server.close(() => process.exit(0)); });
}
