import WebSocket from "ws"

const token = process.env.UPSTOX_ACCESS_TOKEN

console.log("Token exists:", !!token)

const ws = new WebSocket(
  "wss://api.upstox.com/v2/feed/market-data",
  {
    headers: {
      "Authorization": `Bearer ${token}`,
      "Api-Version": "2.0",
      "Accept": "application/json"
    }
  }
)

ws.on("open", () => {
  console.log("✅ Connected to Upstox WebSocket")
})

ws.on("error", (err) => {
  console.error("WebSocket Error:", err.message)
})

ws.on("close", (code, reason) => {
  console.log("Closed:", code, reason.toString())
})
