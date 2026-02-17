import WebSocket from "ws"
import fetch from "node-fetch"

const token = process.env.UPSTOX_ACCESS_TOKEN

console.log("Token exists:", !!token)

async function start() {
  try {
    // Step 1: Get authorized WS URL
    const response = await fetch(
      "https://api.upstox.com/v2/feed/market-data-feed/authorize",
      {
        method: "GET",
        headers: {
          "Accept": "application/json",
          "Authorization": `Bearer ${token}`
        }
      }
    )

    const data = await response.json()

    const wsUrl = data.data.authorized_redirect_uri

    console.log("Authorized WS URL received")

    // Step 2: Connect to that URL
    const ws = new WebSocket(wsUrl)

    ws.on("open", () => {
      console.log("✅ Connected to Upstox WebSocket")
    })

    ws.on("message", (msg) => {
      console.log("Message:", msg.toString())
    })

    ws.on("error", (err) => {
      console.error("WebSocket Error:", err.message)
    })

    ws.on("close", (code, reason) => {
      console.log("Closed:", code, reason.toString())
    })

  } catch (err) {
    console.error("Error:", err)
  }
}

start()

