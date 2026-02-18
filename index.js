import http from "http"

// Dummy HTTP server for Render health checks
const PORT = process.env.PORT || 3000
http.createServer((req, res) => {
  const status = {
    service: "NIFTY LTP Service",
    status: ws && ws.readyState === WebSocket.OPEN ? "connected" : "disconnected",
    atm: currentATM,
    spot: currentSpot,
    instruments: Object.keys(ltpMap).length,
    uptime: process.uptime()
  }
  res.writeHead(200, { "Content-Type": "application/json" })
  res.end(JSON.stringify(status, null, 2))
}).listen(PORT, () => {
  console.log(`Health server listening on port ${PORT}`)
})

import WebSocket from "ws"
import protobuf from "protobufjs"
import { v4 as uuidv4 } from "uuid"
import zlib from "zlib"
import path from "path"
import { fileURLToPath } from "url"
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

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
let instrumentLookup = {}
let currentATM     = null
let currentSpot    = null
let currentExpiry  = null
let ws             = null
let FeedResponse   = null
let pingInterval   = null
let ltpMap         = {}

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
  if (!spotData) throw new Error(`Spot data missing`)

  return spotData.last_price
}

function getATM(spot) {
  return Math.round(spot / STRIKE_GAP) * STRIKE_GAP
}

function generateStrikes(atm) {
  const strikes = []
  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    strikes.push(atm + i * STRIKE_GAP)
  }
  return strikes
}

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

function getNearestExpiry() {
  const now = Date.now()

  const niftyExpiries = instrumentMap
    .filter(i =>
      i.underlying_symbol === "NIFTY" &&
      i.segment === "NSE_FO" &&
      Number(i.expiry) > now
    )
    .map(i => Number(i.expiry))
    .sort((a, b) => a - b)

  if (!niftyExpiries.length) throw new Error("No future NIFTY expiries found")

  return niftyExpiries[0]
}

function getOptionKeys(strikes, expiryMs) {
  const filtered = instrumentMap.filter(i =>
    i.underlying_symbol === "NIFTY" &&
    i.segment === "NSE_FO" &&
    Number(i.expiry) === expiryMs &&
    (i.instrument_type === "CE" || i.instrument_type === "PE") &&
    strikes.includes(Number(i.strike_price))
  )

  if (!filtered.length) {
    throw new Error(`No instruments found for expiry ${new Date(expiryMs).toISOString()}`)
  }

  instrumentLookup = {}
  filtered.forEach(inst => {
    instrumentLookup[inst.instrument_key] = inst
  })

  return filtered.map(i => i.instrument_key)
}

async function loadProto() {
  const protoPath = path.join(__dirname, "MarketDataFeed.proto")
  const root = await protobuf.load(protoPath)
  FeedResponse = root.lookupType(
    "com.upstox.marketdatafeed.rpc.proto.FeedResponse"
  )
  console.log("Protobuf schema loaded")
}

async function handleMessage(buffer) {
  try {
    // Use lenient decoding that skips unknown fields
    const decoded = FeedResponse.decode(new Uint8Array(buffer))
    const feeds   = decoded?.feeds

    if (!feeds || Object.keys(feeds).length === 0) {
      return
    }

    const now = new Date().toISOString()
    const batch = []

    for (const key in feeds) {
      const feed = feeds[key]

      // Try all possible LTP paths
      const ltp = 
        feed?.ltpc?.ltp ??
        feed?.FF?.marketFF?.ltpc?.ltp ??
        feed?.FF?.indexFF?.ltpc?.ltp ??
        feed?.marketQuotes?.ltpc?.ltp

      if (ltp !== undefined && ltp !== null && ltp > 0) {
        // Only log if LTP changed significantly (avoid spam)
        if (!ltpMap[key] || Math.abs(ltpMap[key] - ltp) > 0.01) {
          ltpMap[key] = ltp
          console.log(`${key}  LTP: ${ltp}`)
        }

        const inst = instrumentLookup[key]
        if (inst) {
          batch.push({
            instrument_key: key,
            strike_price: Number(inst.strike_price),
            option_type: inst.instrument_type,
            expiry_date: new Date(currentExpiry).toISOString(),
            ltp: ltp,
            spot_price: currentSpot,
            timestamp: now
          })
        }
      }
    }

    if (batch.length > 0) {
      const { error } = await supabase
        .from('nifty_option_ltp')
        .insert(batch)

      if (error) {
        console.error("Supabase error:", error.message)
      } else {
        console.log(`✅ ${batch.length} rows inserted`)
      }
    }

  } catch (err) {
    // Silently skip decode errors - they happen with partial/malformed frames
    if (err.message && !err.message.includes('index out of range')) {
      console.error("Decode error:", err.message)
    }
  }
}

function clearPing() {
  if (pingInterval) {
    clearInterval(pingInterval)
    pingInterval = null
  }
}

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
    console.log("✅ WebSocket connected")

    ws.send(JSON.stringify({
      guid: uuidv4(),
      method: "subscribe",
      data: {
        mode: "ltp",
        instrumentKeys: keys
      }
    }))
    console.log(`📡 Subscribed to ${keys.length} instruments`)

    clearPing()
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping()
      }
    }, 20000)

    setInterval(async () => {
      try {
        const spot   = await getNiftySpot()
        currentSpot  = spot
        const newATM = getATM(spot)
        if (Math.abs(newATM - currentATM) >= ATM_DRIFT_THRESHOLD) {
          console.log(`ATM drift: ${currentATM} -> ${newATM}`)
          currentATM = newATM
          ws.close()
        }
      } catch (err) {
        console.warn("ATM check failed:", err.message)
      }
    }, 30000)
  })

  ws.on("message", handleMessage)

  ws.on("error", (err) => {
    console.error("WS error:", err.message)
  })

  ws.on("close", (code) => {
    clearPing()
    console.log(`WS closed (${code}). Reconnecting in ${RECONNECT_DELAY / 1000}s...`)
    scheduleRestart()
  })
}

function scheduleRestart() {
  setTimeout(start, RECONNECT_DELAY)
}

async function start() {
  try {
    console.log("🚀 Starting NIFTY LTP Service")

    if (!FeedResponse) await loadProto()
    if (!instrumentMap.length) await loadInstruments()

    const spot      = await getNiftySpot()
    currentSpot     = spot
    console.log("NIFTY Spot:", spot)

    const atm       = getATM(spot)
    currentATM      = atm
    console.log("ATM Strike:", atm)

    const strikes   = generateStrikes(atm)
    const expiryMs  = getNearestExpiry()
    currentExpiry   = expiryMs
    console.log("Expiry:", new Date(expiryMs).toLocaleString())

    const keys      = getOptionKeys(strikes, expiryMs)
    console.log("Instruments:", keys.length)

    await connectWebSocket(keys)

  } catch (err) {
    console.error("Startup error:", err.message)
    scheduleRestart()
  }
}

start()
