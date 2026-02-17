import WebSocket from "ws"

const UPSTOX_ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN

console.log("Token exists:", !!UPSTOX_ACCESS_TOKEN)

const ws = new WebSocket("wss://api.upstox.com/feed/market-data", {
  headers: {
    "Authorization": `Bearer ${UPSTOX_ACCESS_TOKEN}`,
    "Api-Version": "2.0"
  }
})

ws.on("open", () => {
  console.log("✅ Connected Successfully")
})

ws.on("error", (err) => {
  console.error("WebSocket Error:", err.message)
})
