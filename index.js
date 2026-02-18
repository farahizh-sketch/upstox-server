import { createClient } from '@supabase/supabase-js'
import http from "http"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TOKEN = process.env.UPSTOX_ACCESS_TOKEN
const POLL_INTERVAL = 1000  // Poll every 1 second

// Health server
const PORT = process.env.PORT || 3000
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" })
  res.end(JSON.stringify({ status: "running", uptime: process.uptime() }))
}).listen(PORT, () => console.log(`✅ Health server on port ${PORT}`))

async function fetchAndStoreOptionChain() {
  try {
    // Get today's date for nearest expiry
    const today = new Date()
    const expiryDate = getNextThursday(today)  // NIFTY expires on Thursdays
    
    const url = new URL('https://api.upstox.com/v2/option/chain')
    url.searchParams.append('instrument_key', 'NSE_INDEX|Nifty 50')
    url.searchParams.append('expiry_date', expiryDate)

    const res = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'Authorization': `Bearer ${TOKEN}`
      }
    })

    const data = await res.json()
    
    if (data.status !== 'success') {
      console.error('API error:', data)
      return
    }

    const batch = []
    const now = new Date().toISOString()

    // Process option chain data
    for (const item of data.data) {
      if (item.call_options?.market_data?.ltp) {
        batch.push({
          instrument_key: item.call_options.instrument_key,
          strike_price: item.strike_price,
          option_type: 'CE',
          expiry_date: expiryDate,
          ltp: item.call_options.market_data.ltp,
          spot_price: data.data[0]?.underlying_spot_price || null,
          timestamp: now
        })
      }
      
      if (item.put_options?.market_data?.ltp) {
        batch.push({
          instrument_key: item.put_options.instrument_key,
          strike_price: item.strike_price,
          option_type: 'PE',
          expiry_date: expiryDate,
          ltp: item.put_options.market_data.ltp,
          spot_price: data.data[0]?.underlying_spot_price || null,
          timestamp: now
        })
      }
    }

    if (batch.length > 0) {
      const { error } = await supabase
        .from('nifty_option_ltp')
        .insert(batch)

      if (error) {
        console.error('Supabase error:', error.message)
      } else {
        console.log(`✅ ${batch.length} rows inserted`)
      }
    }

  } catch (err) {
    console.error('Error:', err.message)
  }
}

function getNextThursday(date) {
  const day = date.getDay()
  const thursday = 4
  const daysUntilThursday = (thursday - day + 7) % 7 || 7
  const nextThursday = new Date(date)
  nextThursday.setDate(date.getDate() + daysUntilThursday)
  
  return nextThursday.toISOString().split('T')[0]  // YYYY-MM-DD
}

// Poll every second during market hours
console.log('🚀 Starting Option Chain Poller')
setInterval(fetchAndStoreOptionChain, POLL_INTERVAL)
fetchAndStoreOptionChain()  // Run immediately
