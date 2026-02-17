import WebSocket from "ws"
import protobuf from "protobufjs"
import { v4 as uuidv4 } from "uuid"

const TOKEN = process.env.UPSTOX_ACCESS_TOKEN

if (!TOKEN) {
  console.error("❌ UPSTOX_ACCESS_TOKEN missing")
  process.exit(1)
}

const STRIKE_GAP = 50
const STRIKE_RANGE = 9
const NIFTY_KEY = "NSE_INDEX|Nifty 50"

let instrumentMap = []
let currentATM = null
let ws = null
let FeedResponse = null

// ==========================
// 1️⃣ Get NIFTY Spot Price
// ==========================
async function getNiftySpot() {
  const res = await fetch(
    `https://api.upstox.com/v3/market-quote/ltp?instrument_key=${encodeURIComponent(NIFTY_KEY)}`,
    {
      headers: {
        "Accept": "application/json",
        "Authorization": `Bearer ${TOKEN}`
      }
    }
  )

  const data = await res.json()
  return data.data[NIFTY_KEY].last_price
}

// ==========================
// 2️⃣ Calculate ATM
// ==========================
function getATM(spot) {
  return Math.round(spot / STRIKE_GAP) * STRIKE_GAP
}

// ==========================
// 3️⃣ Generate ±9 strikes
// ==========================
function generateStrikes(atm) {
  const strikes = []
  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    strikes.push(atm + i * STRIKE_GAP)
  }
  return strikes
}

// ==========================
// 4️⃣ Load Instruments (once)
// ==========================
async function loadInstruments() {
  console.log("Loading instruments...")
  const res = await fetch(
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE_FO.json"
  )
  instrumentMap = await res.json()
  console.log("Instruments loaded:", instrumentMap.length)
}

// ==========================
// 5️⃣ Get Nearest Expiry
// ==========================
function getNearestExpiry() {
  const today = new Date()
  const niftyOptions = instrumentMap.filter(
    i => i.name === "NIFTY" && i.segment === "NSE_FO"
  )

  const expiries = [...new Set(niftyOptions.map(i => i.expiry))]
  expiries.sort((a, b) => new Date(a) - new Date(b))

  return expiries.find(e => new Date(e) >= today)
}

// ==========================
// 6️⃣ Filter Option Keys
// ==========================
function getOptionKeys(strikes, expiry) {
  const filtered = instrumentMap.filter(i =>
    i.name === "NIFTY" &&
    i.segment === "NSE_FO" &&
    i.expiry === expiry &&
    strikes.includes(Number(i.strike_price))
  )

  return filtered.map(i => i.instrument_key)
}

// ==========================
// 7️⃣ Connect WebSocket
// ==========================
async function connectWebSocket(keys) {

  const authRes = await fetch(
    "https://api.upstox.com/v3/feed/market-data-feed/authorize",
    {
      headers: {
        "Accept": "application/json",
        "Authorization": `Bearer ${TOKEN}`
      }
    }
  )

  const authData = await authRes.json()

  if (!authData.data) {
    console.error("Authorize failed:", authData)
    process.exit(1)
  }

  const wsUrl = authData.data.authorized_redirect_uri
  ws = new WebSocket(wsUrl)

  ws.on("open", () => {
    console.log("✅ WebSocket Connected")

    const subscribeMessage = {
      guid: uuidv4(),
      method: "subscribe",
      data: {
        mode: "ltp",
        instrumentKeys: keys
      }
    }

    ws.send(JSON.stringify(subscribeMessage))
    console.log("📡 Subscribed to", keys.length, "instruments")
  })

  ws.on("message", (buffer) => {
    try {
      const decoded = FeedResponse.decode(new Uint8Array(buffer))
      const feeds = decoded.feeds

      for (const key in feeds) {
        const ltp = feeds[key]?.ltp?.ltp
        if (ltp) {
          console.log(key, "LTP:", ltp)
        }
      }

    } catch (err) {
      // Ignore non-data frames
    }
  })

  ws.on("error", (err) => {
    console.error("WS Error:", err.message)
  })

  ws.on("close", () => {
    console.log("🔴 WS Closed. Reconnecting in 5 sec...")
    setTimeout(start, 5000)
  })
}

// ==========================
// MAIN START
// ==========================
async function start() {
  try {
    console.log("🚀 Starting NIFTY Option Chain LTP Service")

    await loadInstruments()

    const spot = await getNiftySpot()
    console.log("NIFTY Spot:", spot)

    const atm = getATM(spot)
    currentATM = atm
    console.log("ATM Strike:", atm)

    const strikes = generateStrikes(atm)
    const expiry = getNearestExpiry()

    console.log("Nearest Expiry:", expiry)

    const keys = getOptionKeys(strikes, expiry)

    console.log("Total Instruments:", keys.length)

    await protobuf.load("MarketDataFeed.proto")
      .then(root => {
        FeedResponse = root.lookupType(
          "com.upstox.marketdatafeed.rpc.proto.FeedResponse"
        )
      })

    await connectWebSocket(keys)

  } catch (err) {
    console.error("Fatal Error:", err)
    process.exit(1)
  }
}

start()
