import WebSocket from "ws"
import { createClient } from "@supabase/supabase-js"
import dotenv from "dotenv"

dotenv.config()

// Environment Variables
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const UPSTOX_ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN

// Create Supabase client
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
)

// Correct Upstox v2 WebSocket URL
const WS_URL = "wss://api.upstox.com/v2/feed/market-data"

// Connect to Upstox
const ws = new WebSocket(WS_URL, {
  headers: {
    Authorization: `Bearer ${UPSTOX_ACCESS_TOKEN}`
  }
})

ws.on("open", () => {
  console.log("✅ Connected to Upstox WebSocket")

  // Subscribe to instrument
  ws.send(
    JSON.stringify({
      guid: "some-guid",
      method: "sub",
      data: {
        mode: "ltp",  // ltp | full | option_chain
        instrumentKeys: ["NSE_EQ|INE002A01018"] // Example: RELIANCE
      }
    })
  )
})

ws.on("message", async (data) => {
  try {
    const parsed = JSON.parse(data.toString())

    console.log("Tick:", parsed)

    // Extract price safely
    const feed = parsed?.data?.feeds
    if (!feed) return

    for (const key in feed) {
      const ltp = feed[key]?.ltp?.ltp
      if (!ltp) continue

      await supabase.from("live_ticks").insert([
        {
          symbol: key,
          price: ltp,
          volume: 0
        }
      ])
    }
  } catch (err) {
    console.error("Parse Error:", err.message)
  }
})

ws.on("error", (err) => {
  console.error("WebSocket Error:", err.message)
})

ws.on("close", () => {
  console.log("❌ WebSocket Closed")
})
