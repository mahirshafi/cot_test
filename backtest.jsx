import { useState, useCallback } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer, Legend } from "recharts";

// ── Replicate signal engine from dashboard ──────────────────────────────────
function getZone(idx) {
  if (idx >= 75) return "EXTREME_LONG";
  if (idx <= 25) return "EXTREME_SHORT";
  return "NEUTRAL";
}

function isMomentumUnwinding(trendUp, weekChange, zone) {
  if (zone === "EXTREME_LONG")  return !trendUp || weekChange < 0;
  if (zone === "EXTREME_SHORT") return trendUp  || weekChange > 0;
  return false;
}

function computeSignal(base, quote) {
  const bIdx = base.cotIndex, qIdx = quote.cotIndex;
  const bZone = getZone(bIdx), qZone = getZone(qIdx);
  const spread = bIdx - qIdx;

  const bothExtremeLong  = bZone === "EXTREME_LONG"  && qZone === "EXTREME_LONG";
  const bothExtremeShort = bZone === "EXTREME_SHORT" && qZone === "EXTREME_SHORT";

  if (bothExtremeLong || bothExtremeShort) {
    return { direction: spread > 0 ? "BUY" : "SELL", conviction: 2, signalType: "CONFLICT" };
  }

  let baseSignal = null, quoteSignal = null;
  if (bZone === "EXTREME_LONG")  baseSignal  = "SELL";
  if (bZone === "EXTREME_SHORT") baseSignal  = "BUY";
  if (qZone === "EXTREME_LONG")  quoteSignal = "BUY";
  if (qZone === "EXTREME_SHORT") quoteSignal = "SELL";

  let direction, conviction = 0, signalType;

  if (baseSignal && quoteSignal && baseSignal === quoteSignal) {
    direction = baseSignal; signalType = "CONTRARIAN"; conviction += 5;
  } else if (baseSignal && !quoteSignal) {
    direction = baseSignal; signalType = "CONTRARIAN"; conviction += 3;
  } else if (!baseSignal && quoteSignal) {
    direction = quoteSignal; signalType = "CONTRARIAN"; conviction += 3;
  } else {
    direction = spread >= 0 ? "BUY" : "SELL"; signalType = "TREND";
    const abs = Math.abs(spread);
    if (abs >= 40) conviction += 2;
    else if (abs >= 20) conviction += 1;
  }

  const baseMoving  = direction === "BUY" ? base.trendUp  : !base.trendUp;
  const quoteMoving = direction === "BUY" ? !quote.trendUp : quote.trendUp;
  if (baseMoving && quoteMoving) conviction += 2;
  else if (baseMoving || quoteMoving) conviction += 1;

  if (bZone !== "NEUTRAL" && isMomentumUnwinding(base.trendUp, base.weekChange, bZone)) conviction += 1;
  if (qZone !== "NEUTRAL" && isMomentumUnwinding(quote.trendUp, quote.weekChange, qZone)) conviction += 1;

  const bWk = direction === "BUY" ? base.weekChange > 0  : base.weekChange < 0;
  const qWk = direction === "BUY" ? quote.weekChange < 0 : quote.weekChange > 0;
  if (bWk && qWk) conviction += 1;

  return { direction, conviction: Math.min(conviction, 9), signalType };
}

// ── Reconstruct rolling COT index per week (no lookahead) ──────────────────
function buildWeeklySignals(baseWeeks, quoteWeeks) {
  // weeks are newest-first — reverse to chronological
  const bChron = [...baseWeeks].reverse();
  const qChron = [...quoteWeeks].reverse();
  const len = Math.min(bChron.length, qChron.length);

  const signals = [];
  for (let i = 2; i < len; i++) {
    const bSlice = bChron.slice(Math.max(0, i - 51), i + 1); // up to 52 weeks ending at i
    const qSlice = qChron.slice(Math.max(0, i - 51), i + 1);

    const bNets = bSlice.map(w => w.net_noncomm);
    const qNets = qSlice.map(w => w.net_noncomm);

    const bMax = Math.max(...bNets), bMin = Math.min(...bNets);
    const qMax = Math.max(...qNets), qMin = Math.min(...qNets);

    const bCur = bNets[bNets.length - 1];
    const qCur = qNets[qNets.length - 1];

    const bIdx = bMax === bMin ? 50 : Math.round(((bCur - bMin) / (bMax - bMin)) * 100);
    const qIdx = qMax === qMin ? 50 : Math.round(((qCur - qMin) / (qMax - qMin)) * 100);

    const bPrev = bNets[bNets.length - 2] || bCur;
    const bPrev2 = bNets[bNets.length - 3] || bPrev;
    const qPrev = qNets[qNets.length - 2] || qCur;
    const qPrev2 = qNets[qNets.length - 3] || qPrev;

    const bLast4  = bNets.slice(-4), bLast13 = bNets.slice(-13);
    const qLast4  = qNets.slice(-4), qLast13 = qNets.slice(-13);
    const bMa4  = bLast4.reduce((a,b)=>a+b,0)/bLast4.length;
    const bMa13 = bLast13.reduce((a,b)=>a+b,0)/bLast13.length;
    const qMa4  = qLast4.reduce((a,b)=>a+b,0)/qLast4.length;
    const qMa13 = qLast13.reduce((a,b)=>a+b,0)/qLast13.length;

    const baseSnap  = { cotIndex: bIdx, trendUp: bMa4 > bMa13, weekChange: bCur - bPrev, prevWeekChange: bPrev - bPrev2 };
    const quoteSnap = { cotIndex: qIdx, trendUp: qMa4 > qMa13, weekChange: qCur - qPrev, prevWeekChange: qPrev - qPrev2 };

    const sig = computeSignal(baseSnap, quoteSnap);

    signals.push({
      date: bChron[i].date,
      bIdx, qIdx,
      bNet: bCur, qNet: qCur,
      spread: bIdx - qIdx,
      ...sig,
    });
  }
  return signals;
}

// ── Fetch FX price from Frankfurter API ────────────────────────────────────
async function fetchFxPrices(from, to, startDate, endDate) {
  // Frankfurter is CORS-friendly, free, no auth needed
  // Handle inverted pairs — if USD is base, use inverse
  const url = `https://api.frankfurter.app/${startDate}..${endDate}?from=${from}&to=${to}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`FX fetch failed: ${res.status}`);
  const data = await res.json();
  // Returns { rates: { "2025-01-07": { EUR: 1.02 }, ... } }
  return Object.entries(data.rates)
    .sort(([a],[b]) => a.localeCompare(b))
    .map(([date, rates]) => ({ date, price: rates[to] || rates[Object.keys(rates)[0]] }));
}

// ── Match COT signal to nearest FX price & next week price ─────────────────
function matchSignalsToPrice(signals, prices) {
  // For each signal date, find the Friday close and the following Friday close
  return signals.map(sig => {
    // Find price on or just after signal date
    const entry = prices.find(p => p.date >= sig.date);
    // Find price ~7 days later
    const exitDate = new Date(sig.date);
    exitDate.setDate(exitDate.getDate() + 9); // +9 days to cover weekend
    const exitDateStr = exitDate.toISOString().slice(0, 10);
    const exit = prices.find(p => p.date >= exitDateStr);

    if (!entry || !exit) return { ...sig, entryPrice: null, exitPrice: null, result: null };

    const pctChange = ((exit.price - entry.price) / entry.price) * 100;
    // BUY signal = we want price to go UP = positive pct = WIN
    const result = sig.direction === "BUY"
      ? pctChange > 0 ? "WIN" : pctChange < 0 ? "LOSS" : "FLAT"
      : pctChange < 0 ? "WIN" : pctChange > 0 ? "LOSS" : "FLAT";

    return { ...sig, entryPrice: entry.price, exitPrice: exit.price, pctChange: +pctChange.toFixed(3), result };
  }).filter(s => s.result !== null);
}

// ── FX pair mapping for Frankfurter API ────────────────────────────────────
const FX_MAP = {
  "EUR/USD": { from: "EUR", to: "USD" },
  "EUR/GBP": { from: "EUR", to: "GBP" },
  "EUR/JPY": { from: "EUR", to: "JPY" },
  "EUR/CHF": { from: "EUR", to: "CHF" },
  "EUR/CAD": { from: "EUR", to: "CAD" },
  "EUR/AUD": { from: "EUR", to: "AUD" },
  "EUR/NZD": { from: "EUR", to: "NZD" },
  "GBP/USD": { from: "GBP", to: "USD" },
  "GBP/JPY": { from: "GBP", to: "JPY" },
  "GBP/CHF": { from: "GBP", to: "CHF" },
  "GBP/CAD": { from: "GBP", to: "CAD" },
  "GBP/AUD": { from: "GBP", to: "AUD" },
  "GBP/NZD": { from: "GBP", to: "NZD" },
  "USD/JPY": { from: "USD", to: "JPY" },
  "USD/CHF": { from: "USD", to: "CHF" },
  "USD/CAD": { from: "USD", to: "CAD" },
  "AUD/USD": { from: "AUD", to: "USD" },
  "NZD/USD": { from: "NZD", to: "USD" },
  "AUD/JPY": { from: "AUD", to: "JPY" },
  "AUD/NZD": { from: "AUD", to: "NZD" },
  "CAD/JPY": { from: "CAD", to: "JPY" },
  "CHF/JPY": { from: "CHF", to: "JPY" },
  "NZD/JPY": { from: "NZD", to: "JPY" },
  "AUD/CAD": { from: "AUD", to: "CAD" },
};

// ── Main App ────────────────────────────────────────────────────────────────
export default function BacktestApp() {
  const [cotUrl, setCotUrl]     = useState("https://mahirshafi.github.io/cot_test/cot_data.json");
  const [pair, setPair]         = useState("EUR/USD");
  const [minConv, setMinConv]   = useState(4);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState(null);
  const [results, setResults]   = useState(null);
  const [chartData, setChartData] = useState(null);

  const PAIRS = Object.keys(FX_MAP);

  const runBacktest = useCallback(async () => {
    setLoading(true); setError(null); setResults(null); setChartData(null);
    try {
      // 1. Fetch COT data
      const cotRes = await fetch(cotUrl);
      if (!cotRes.ok) throw new Error(`Could not fetch COT data: ${cotRes.status}`);
      const cot = await cotRes.json();

      const [baseCode, quoteCode] = pair.split("/");
      // Handle USD — USD in JSON might be named differently
      const baseData  = cot.data[baseCode];
      const quoteData = cot.data[quoteCode];

      if (!baseData)  throw new Error(`No COT data for ${baseCode}`);
      if (!quoteData) throw new Error(`No COT data for ${quoteCode}`);

      // 2. Build weekly signals
      const signals = buildWeeklySignals(baseData.weeks, quoteData.weeks);
      if (signals.length === 0) throw new Error("Not enough history to build signals");

      // 3. Fetch FX prices
      const fxMap = FX_MAP[pair];
      const startDate = signals[0].date;
      const endDate   = signals[signals.length - 1].date;
      // Add 2 weeks to endDate for exit prices
      const endPlus = new Date(endDate);
      endPlus.setDate(endPlus.getDate() + 14);
      const endPlusStr = endPlus.toISOString().slice(0, 10);

      let prices;
      try {
        prices = await fetchFxPrices(fxMap.from, fxMap.to, startDate, endPlusStr);
      } catch(e) {
        throw new Error(`FX price fetch failed for ${pair}: ${e.message}`);
      }

      // 4. Match signals to prices
      const matched = matchSignalsToPrice(signals, prices);

      // 5. Filter by conviction
      const filtered = matched.filter(s => s.conviction >= minConv && s.signalType !== "CONFLICT");

      // 6. Compute stats
      const wins   = filtered.filter(s => s.result === "WIN").length;
      const losses = filtered.filter(s => s.result === "LOSS").length;
      const total  = wins + losses;
      const winRate = total > 0 ? ((wins / total) * 100).toFixed(1) : 0;

      // By signal type
      const contrarian = filtered.filter(s => s.signalType === "CONTRARIAN");
      const trend      = filtered.filter(s => s.signalType === "TREND");
      const cWins = contrarian.filter(s => s.result === "WIN").length;
      const tWins = trend.filter(s => s.result === "WIN").length;

      // Avg pct change on wins vs losses
      const avgWin  = filtered.filter(s => s.result === "WIN").reduce((a,b)=>a+Math.abs(b.pctChange),0) / (wins||1);
      const avgLoss = filtered.filter(s => s.result === "LOSS").reduce((a,b)=>a+Math.abs(b.pctChange),0) / (losses||1);

      // Equity curve (cumulative pct, 1 unit per trade)
      let equity = 0;
      const equity_curve = filtered.map(s => {
        equity += s.result === "WIN" ? Math.abs(s.pctChange) : -Math.abs(s.pctChange);
        return { date: s.date, equity: +equity.toFixed(3) };
      });

      // COT index chart
      const cotChart = signals.map(s => ({ date: s.date, base: s.bIdx, quote: s.qIdx }));

      setResults({ filtered, total, wins, losses, winRate, contrarian, trend, cWins, tWins, avgWin: avgWin.toFixed(3), avgLoss: avgLoss.toFixed(3), equity_curve });
      setChartData({ cotChart, equity_curve });

    } catch(e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [cotUrl, pair, minConv]);

  const resultColor = r => r === "WIN" ? "#10b981" : r === "LOSS" ? "#ef4444" : "#6b7280";
  const typeColor   = t => t === "CONTRARIAN" ? "#f59e0b" : t === "TREND" ? "#60a5fa" : "#6b7280";

  return (
    <div style={{ fontFamily: "'Inter', sans-serif", background: "#0f1117", minHeight: "100vh", color: "#e2e8f0", padding: "24px" }}>
      <div style={{ maxWidth: 900, margin: "0 auto" }}>

        {/* Header */}
        <div style={{ marginBottom: 24 }}>
          <h1 style={{ fontSize: 22, fontWeight: 700, color: "#fff", margin: 0 }}>COT Signal Backtest</h1>
          <p style={{ fontSize: 13, color: "#64748b", marginTop: 4 }}>
            Reconstructs rolling COT signals with no lookahead bias. Compares against real weekly FX price moves.
          </p>
        </div>

        {/* Controls */}
        <div style={{ background: "#1e2130", border: "1px solid #2d3348", borderRadius: 12, padding: 20, marginBottom: 24 }}>
          <div style={{ display: "flex", gap: 16, flexWrap: "wrap", alignItems: "flex-end" }}>
            <div style={{ flex: 2, minWidth: 240 }}>
              <label style={{ fontSize: 11, color: "#64748b", display: "block", marginBottom: 6 }}>COT DATA URL</label>
              <input value={cotUrl} onChange={e => setCotUrl(e.target.value)}
                style={{ width: "100%", background: "#0f1117", border: "1px solid #2d3348", borderRadius: 8, padding: "8px 12px", color: "#e2e8f0", fontSize: 12, boxSizing: "border-box" }} />
            </div>
            <div style={{ minWidth: 140 }}>
              <label style={{ fontSize: 11, color: "#64748b", display: "block", marginBottom: 6 }}>PAIR</label>
              <select value={pair} onChange={e => setPair(e.target.value)}
                style={{ width: "100%", background: "#0f1117", border: "1px solid #2d3348", borderRadius: 8, padding: "8px 12px", color: "#e2e8f0", fontSize: 13 }}>
                {PAIRS.map(p => <option key={p}>{p}</option>)}
              </select>
            </div>
            <div style={{ minWidth: 120 }}>
              <label style={{ fontSize: 11, color: "#64748b", display: "block", marginBottom: 6 }}>MIN CONVICTION</label>
              <select value={minConv} onChange={e => setMinConv(+e.target.value)}
                style={{ width: "100%", background: "#0f1117", border: "1px solid #2d3348", borderRadius: 8, padding: "8px 12px", color: "#e2e8f0", fontSize: 13 }}>
                {[1,2,3,4,5,6,7,8,9].map(n => <option key={n} value={n}>{n}+</option>)}
              </select>
            </div>
            <button onClick={runBacktest} disabled={loading}
              style={{ background: loading ? "#374151" : "#3b82f6", color: "#fff", border: "none", borderRadius: 8, padding: "9px 22px", fontSize: 13, fontWeight: 600, cursor: loading ? "not-allowed" : "pointer" }}>
              {loading ? "Running..." : "Run Backtest"}
            </button>
          </div>
        </div>

        {error && (
          <div style={{ background: "rgba(239,68,68,.1)", border: "1px solid rgba(239,68,68,.3)", borderRadius: 8, padding: 16, marginBottom: 20, color: "#ef4444", fontSize: 13 }}>
            ⚠ {error}
          </div>
        )}

        {results && (
          <>
            {/* Stats Row */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12, marginBottom: 24 }}>
              {[
                { label: "WIN RATE", value: `${results.winRate}%`, color: +results.winRate >= 55 ? "#10b981" : +results.winRate >= 45 ? "#f59e0b" : "#ef4444" },
                { label: "TOTAL SIGNALS", value: results.total },
                { label: "WINS", value: results.wins, color: "#10b981" },
                { label: "LOSSES", value: results.losses, color: "#ef4444" },
                { label: "AVG WIN %", value: `+${results.avgWin}%`, color: "#10b981" },
                { label: "AVG LOSS %", value: `-${results.avgLoss}%`, color: "#ef4444" },
              ].map(s => (
                <div key={s.label} style={{ background: "#1e2130", border: "1px solid #2d3348", borderRadius: 10, padding: "14px 16px" }}>
                  <div style={{ fontSize: 10, color: "#64748b", marginBottom: 6, letterSpacing: 1 }}>{s.label}</div>
                  <div style={{ fontSize: 22, fontWeight: 700, color: s.color || "#e2e8f0" }}>{s.value}</div>
                </div>
              ))}
            </div>

            {/* By Signal Type */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 24 }}>
              {[
                { label: "⟳ CONTRARIAN signals", total: results.contrarian.length, wins: results.cWins, color: "#f59e0b" },
                { label: "→ TREND signals",       total: results.trend.length,      wins: results.tWins, color: "#60a5fa" },
              ].map(s => (
                <div key={s.label} style={{ background: "#1e2130", border: "1px solid #2d3348", borderRadius: 10, padding: 16 }}>
                  <div style={{ fontSize: 12, color: s.color, fontWeight: 600, marginBottom: 8 }}>{s.label}</div>
                  <div style={{ display: "flex", gap: 16 }}>
                    <span style={{ fontSize: 13, color: "#e2e8f0" }}>{s.total} signals</span>
                    <span style={{ fontSize: 13, color: "#10b981" }}>{s.wins}W / {s.total - s.wins}L</span>
                    <span style={{ fontSize: 13, color: "#64748b" }}>{s.total > 0 ? ((s.wins/s.total)*100).toFixed(0) : 0}%</span>
                  </div>
                </div>
              ))}
            </div>

            {/* Charts */}
            {chartData && (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
                <div style={{ background: "#1e2130", border: "1px solid #2d3348", borderRadius: 12, padding: 16 }}>
                  <div style={{ fontSize: 12, color: "#64748b", marginBottom: 12 }}>COT INDEX HISTORY — {pair}</div>
                  <ResponsiveContainer width="100%" height={180}>
                    <LineChart data={chartData.cotChart}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3348" />
                      <XAxis dataKey="date" tick={{ fontSize: 9, fill: "#64748b" }} tickFormatter={d => d.slice(5)} interval={Math.floor(chartData.cotChart.length / 6)} />
                      <YAxis domain={[0, 100]} tick={{ fontSize: 9, fill: "#64748b" }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3348", fontSize: 11 }} />
                      <ReferenceLine y={75} stroke="#ef4444" strokeDasharray="4 2" label={{ value: "75", fontSize: 9, fill: "#ef4444" }} />
                      <ReferenceLine y={25} stroke="#10b981" strokeDasharray="4 2" label={{ value: "25", fontSize: 9, fill: "#10b981" }} />
                      <Line type="monotone" dataKey="base"  stroke="#f59e0b" dot={false} strokeWidth={2} name={pair.split("/")[0]} />
                      <Line type="monotone" dataKey="quote" stroke="#60a5fa" dot={false} strokeWidth={2} name={pair.split("/")[1]} />
                      <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ background: "#1e2130", border: "1px solid #2d3348", borderRadius: 12, padding: 16 }}>
                  <div style={{ fontSize: 12, color: "#64748b", marginBottom: 12 }}>CUMULATIVE P&L (% units)</div>
                  <ResponsiveContainer width="100%" height={180}>
                    <LineChart data={chartData.equity_curve}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2d3348" />
                      <XAxis dataKey="date" tick={{ fontSize: 9, fill: "#64748b" }} tickFormatter={d => d.slice(5)} interval={Math.floor(chartData.equity_curve.length / 6)} />
                      <YAxis tick={{ fontSize: 9, fill: "#64748b" }} />
                      <Tooltip contentStyle={{ background: "#1e2130", border: "1px solid #2d3348", fontSize: 11 }} formatter={v => [`${v}%`, "P&L"]} />
                      <ReferenceLine y={0} stroke="#64748b" />
                      <Line type="monotone" dataKey="equity" stroke="#10b981" dot={false} strokeWidth={2} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            )}

            {/* Signal Table */}
            <div style={{ background: "#1e2130", border: "1px solid #2d3348", borderRadius: 12, overflow: "hidden" }}>
              <div style={{ padding: "14px 20px", borderBottom: "1px solid #2d3348", fontSize: 12, color: "#64748b" }}>
                SIGNAL LOG — {results.filtered.length} trades (conviction ≥ {minConv}, conflicts excluded)
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr style={{ background: "#161928" }}>
                      {["Date", "Type", "Signal", "Conv", `${pair.split("/")[0]} COT`, `${pair.split("/")[1]} COT`, "Spread", "Entry", "Exit", "Δ%", "Result"].map(h => (
                        <th key={h} style={{ padding: "10px 14px", textAlign: "left", color: "#64748b", fontWeight: 500, whiteSpace: "nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {results.filtered.slice().reverse().map((s, i) => (
                      <tr key={i} style={{ borderTop: "1px solid #1a1f2e", background: i % 2 === 0 ? "transparent" : "rgba(255,255,255,.01)" }}>
                        <td style={{ padding: "9px 14px", color: "#94a3b8" }}>{s.date}</td>
                        <td style={{ padding: "9px 14px", color: typeColor(s.signalType), fontWeight: 600 }}>{s.signalType === "CONTRARIAN" ? "⟳" : "→"} {s.signalType}</td>
                        <td style={{ padding: "9px 14px", color: s.direction === "BUY" ? "#10b981" : "#ef4444", fontWeight: 700 }}>{s.direction}</td>
                        <td style={{ padding: "9px 14px", color: "#e2e8f0" }}>{s.conviction}/9</td>
                        <td style={{ padding: "9px 14px", color: s.bIdx >= 75 ? "#ef4444" : s.bIdx <= 25 ? "#10b981" : "#e2e8f0", fontWeight: s.bIdx >= 75 || s.bIdx <= 25 ? 700 : 400 }}>{s.bIdx}</td>
                        <td style={{ padding: "9px 14px", color: s.qIdx >= 75 ? "#ef4444" : s.qIdx <= 25 ? "#10b981" : "#e2e8f0", fontWeight: s.qIdx >= 75 || s.qIdx <= 25 ? 700 : 400 }}>{s.qIdx}</td>
                        <td style={{ padding: "9px 14px", color: "#94a3b8" }}>{s.spread > 0 ? "+" : ""}{s.spread}</td>
                        <td style={{ padding: "9px 14px", color: "#94a3b8" }}>{s.entryPrice?.toFixed(4)}</td>
                        <td style={{ padding: "9px 14px", color: "#94a3b8" }}>{s.exitPrice?.toFixed(4)}</td>
                        <td style={{ padding: "9px 14px", color: s.pctChange > 0 ? "#10b981" : "#ef4444" }}>{s.pctChange > 0 ? "+" : ""}{s.pctChange}%</td>
                        <td style={{ padding: "9px 14px" }}>
                          <span style={{ background: `${resultColor(s.result)}22`, color: resultColor(s.result), padding: "3px 8px", borderRadius: 6, fontWeight: 700, fontSize: 11 }}>{s.result}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div style={{ marginTop: 16, fontSize: 11, color: "#374151" }}>
              * Each signal uses only data available at that point in time (no lookahead). "Exit" = price ~7 days after signal. Conviction filter excludes CONFLICT signals. FX prices from Frankfurter API (ECB reference rates).
            </div>
          </>
        )}
      </div>
    </div>
  );
}
