import WebSocket from "ws"
import { createClient } from "@supabase/supabase-js"
import dotenv from "dotenv"

dotenv.config()

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const ws = new WebSocket("wss://api.upstox.com/feed/market-data")

ws.on("open", () => {
  console.log("Connected to Upstox")

  ws.send(JSON.stringify({
    action: "subscribe",
    instruments: ["NSE_EQ|INE002A01018"]
  }))
})

ws.on("message", async (data) => {
  const tick = JSON.parse(data)

  await supabase.from("live_ticks").insert([{
    symbol: tick.symbol,
    price: tick.price,
    volume: tick.volume
  }])
})
