import WebSocket from "ws"

const token = process.env.UPSTOX_ACCESS_TOKEN

console.log("Token exists:", !!token)

const ws = new WebSocket(
  `wss://api.upstox.com/v2/feed/market-data?access_token=${token}`
)

ws.on("open", () => {
  console.log("✅ Connected to Upstox")
})

ws.on("error", (err) => {
  console.error("WebSocket Error:", err.message)
})
