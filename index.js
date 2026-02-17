import WebSocket from "ws"
import protobuf from "protobufjs"
import { v4 as uuidv4 } from "uuid"
import zlib from "zlib"
import path from "path"
import { fileURLToPath } from "url"

const __filename = fileURLToPath(import.meta.url)
const __dirname  = path.dirname(__filename)

const TOKEN = process.env.UPSTOX_ACCESS_TOKEN

if (!TOKEN) {
  console.error("UPSTOX_ACCESS_TOKEN missing")
  process.exit(1)
}

const STRIKE_GAP           = 50
const STRIKE_RANGE         = 9
const NIFTY_INDEX_KEY      = "NSE_INDEX|Nifty 50"
const RECONNECT_DELAY      = 5000
const ATM_DRIFT_THRESHOLD  = STRIKE_GAP

let instrumentMap  = []
let currentATM     = null
let ws             = null
let FeedResponse   = null
let pingInterval   = null
let ltpMap         = {}

// ============================================================
// 1. Spot price
// ============================================================
async function getNiftySpot() {
  const url =
    `https://api.upstox.com/v2/market-quote/quotes` +
    `?instrument_key=${encodeURIComponent(NIFTY_INDEX_KEY)}`

  const res = await fetch(url, {
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${TOKEN}`
    }
  })

  const json = await res.json()
  const spotData = json?.data?.["NSE_INDEX:Nifty 50"]
  if (!spotData) throw new Error(`Spot data missing. Response: ${JSON.stringify(json)}`)

  return spotData.last_price
}

// ============================================================
// 2. ATM calculation
// ============================================================
function getATM(spot) {
  return Math.round(spot / STRIKE_GAP) * STRIKE_GAP
}

// ============================================================
// 3. Generate +/-STRIKE_RANGE strikes around ATM
// ============================================================
function generateStrikes(atm) {
  const strikes = []
  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    strikes.push(atm + i * STRIKE_GAP)
  }
  return strikes
}

// ============================================================
// 4. Load NSE instrument master (same URL that works in Python)
// ============================================================
async function loadInstruments() {
  console.log("Loading instruments...")
  const res = await fetch(
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
  )
  if (!res.ok) throw new Error(`Failed to load instruments: ${res.status}`)

  const buffer       = await res.arrayBuffer()
  const decompressed = zlib.gunzipSync(Buffer.from(buffer))
  instrumentMap      = JSON.parse(decompressed.toString("utf-8"))
  console.log(`Instruments loaded: ${instrumentMap.length}`)
}

// ============================================================
// 5. Nearest expiry (expiry field is UNIX ms timestamp)
// ============================================================
function getNearestExpiry() {
  const now = Date.now()

  const niftyExpiries = instrumentMap
    .filter(i =>
      i.underlying_symbol === "NIFTY" &&
      i.segment === "NSE_FO" &&
      Number(i.expiry) > now
    )
    .map(i => Number(i.expiry))

  if (!niftyExpiries.length) throw new Error("No future NIFTY expiries found")

  return Math.min(...niftyExpiries)
}

// ============================================================
// 6. Resolve instrument keys for selected strikes + expiry
// ============================================================
function getOptionKeys(strikes, expiryMs) {
  const filtered = instrumentMap.filter(i =>
    i.underlying_symbol === "NIFTY" &&
    i.segment === "NSE_FO" &&
    Number(i.expiry) === expiryMs &&
    (i.instrument_type === "CE" || i.instrument_type === "PE") &&
    strikes.includes(Number(i.strike_price))
  )

  if (!filtered.length) {
    throw new Error(
      `No instruments found for strikes ${strikes} and expiry ${new Date(expiryMs).toISOString()}`
    )
  }

  return filtered.map(i => i.instrument_key)
}

// ============================================================
// 7. Load protobuf schema (absolute path)
// ============================================================
async function loadProto() {
  const protoPath = path.join(__dirname, "MarketDataFeed.proto")
  console.log("Loading proto from:", protoPath)
  const root = await protobuf.load(protoPath)
  FeedResponse = root.lookupType(
    "com.upstox.marketdatafeed.rpc.proto.FeedResponse"
  )
  console.log("Protobuf schema loaded")
}

// ============================================================
// 8. Decode and handle incoming WS message
// ============================================================
function handleMessage(buffer) {
  try {
    const decoded = FeedResponse.decode(new Uint8Array(buffer))
    const feeds   = decoded?.feeds
    if (!feeds) return

    for (const key in feeds) {
      const ltp =
        feeds[key]?.FF?.marketFF?.ltpc?.ltp ??
        feeds[key]?.ltp?.ltp

      if (ltp !== undefined && ltp !== null) {
        ltpMap[key] = ltp
        console.log(`${key}  LTP: ${ltp}`)
      }
    }
  } catch {
    // non-protobuf frames silently ignored
  }
}

// ============================================================
// 9. Clear ping interval safely
// ============================================================
function clearPing() {
  if (pingInterval) {
    clearInterval(pingInterval)
    pingInterval = null
  }
}

// ============================================================
// 10. Connect WebSocket with heartbeat + ATM drift check
// ============================================================
async function connectWebSocket(keys) {
  const authRes = await fetch(
    "https://api.upstox.com/v3/feed/market-data-feed/authorize",
    {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${TOKEN}`
      }
    }
  )

  const authData = await authRes.json()
  const wsUrl    = authData?.data?.authorized_redirect_uri

  if (!wsUrl) {
    console.error("WebSocket authorize failed:", authData)
    scheduleRestart()
    return
  }

  ws = new WebSocket(wsUrl)

  ws.on("open", () => {
    console.log("WebSocket connected")

    ws.send(JSON.stringify({
      guid: uuidv4(),
      method: "subscribe",
      data: {
        mode: "ltp",
        instrumentKeys: keys
      }
    }))
    console.log(`Subscribed to ${keys.length} instruments`)

    // Heartbeat ping every 20s
    clearPing()
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping()
      }
    }, 20000)

    // ATM drift check every 30s
    setInterval(async () => {
      try {
        const spot   = await getNiftySpot()
        const newATM = getATM(spot)
        if (Math.abs(newATM - currentATM) >= ATM_DRIFT_THRESHOLD) {
          console.log(`ATM drifted ${currentATM} -> ${newATM}. Reconnecting...`)
          currentATM = newATM
          ws.close()
        }
      } catch (err) {
        console.warn("ATM drift check failed:", err.message)
      }
    }, 30000)
  })

  ws.on("message", handleMessage)

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message)
  })

  ws.on("close", (code) => {
    clearPing()
    console.log(`WebSocket closed (${code}). Reconnecting in ${RECONNECT_DELAY / 1000}s...`)
    scheduleRestart()
  })
}

// ============================================================
// 11. Delayed restart
// ============================================================
function scheduleRestart() {
  setTimeout(start, RECONNECT_DELAY)
}

// ============================================================
// MAIN
// ============================================================
async function start() {
  try {
    console.log("Starting NIFTY Option Chain LTP Service")

    if (!FeedResponse) await loadProto()
    if (!instrumentMap.length) await loadInstruments()

    const spot     = await getNiftySpot()
    console.log("NIFTY Spot:", spot)

    const atm      = getATM(spot)
    currentATM     = atm
    console.log("ATM Strike:", atm)

    const strikes  = generateStrikes(atm)
    const expiryMs = getNearestExpiry()
    console.log("Nearest Expiry:", new Date(expiryMs).toLocaleString())

    const keys     = getOptionKeys(strikes, expiryMs)
    console.log("Total instruments to subscribe:", keys.length)

    await connectWebSocket(keys)

  } catch (err) {
    console.error("Startup error:", err.message)
    scheduleRestart()
  }
}

start()
