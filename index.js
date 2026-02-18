import { createClient } from '@supabase/supabase-js'
import http from "http"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const TOKEN = process.env.UPSTOX_ACCESS_TOKEN
const POLL_INTERVAL = 2000  // Poll every 2 seconds

if (!TOKEN) {
  console.error("❌ UPSTOX_ACCESS_TOKEN missing")
  process.exit(1)
}

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Supabase credentials missing")
  process.exit(1)
}

// Health server
const PORT = process.env.PORT || 3000
let lastFetchTime = null
let lastRowCount = 0

http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" })
  res.end(JSON.stringify({ 
    status: "running", 
    uptime: process.uptime(),
    lastFetch: lastFetchTime,
    lastRowCount: lastRowCount
  }))
}).listen(PORT, () => console.log(`✅ Health server on port ${PORT}`))

async function fetchAndStoreOptionChain() {
  try {
    console.log("📡 Fetching option chain...")
    
    // Get next Thursday expiry
    const expiryDate = getNextThursday(new Date())
    console.log(`Using expiry: ${expiryDate}`)
    
    const url = new URL('https://api.upstox.com/v2/option/chain')
    url.searchParams.append('instrument_key', 'NSE_INDEX|Nifty 50')
    url.searchParams.append('expiry_date', expiryDate)

    const res = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'Authorization': `Bearer ${TOKEN}`
      }
    })

    if (!res.ok) {
      console.error(`❌ HTTP ${res.status}: ${res.statusText}`)
      const text = await res.text()
      console.error(`Response: ${text}`)
      return
    }

    const data = await res.json()
    console.log(`API response status: ${data.status}`)
    
    if (data.status !== 'success') {
      console.error('❌ API error:', JSON.stringify(data, null, 2))
      return
    }

    if (!data.data || data.data.length === 0) {
      console.warn('⚠️  No option chain data returned')
      return
    }

    console.log(`Got ${data.data.length} strikes`)

    const batch = []
    const now = new Date().toISOString()
    const spotPrice = data.data[0]?.underlying_spot_price || null

    console.log(`Spot price: ${spotPrice}`)

    // Process option chain data
    for (const item of data.data) {
      if (item.call_options?.market_data?.ltp) {
        batch.push({
          instrument_key: item.call_options.instrument_key,
          strike_price: item.strike_price,
          option_type: 'CE',
          expiry_date: expiryDate,
          ltp: item.call_options.market_data.ltp,
          spot_price: spotPrice,
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
          spot_price: spotPrice,
          timestamp: now
        })
      }
    }

    console.log(`Prepared ${batch.length} rows for insert`)

    if (batch.length > 0) {
      const { data: insertData, error } = await supabase
        .from('nifty_option_ltp')
        .insert(batch)

      if (error) {
        console.error('❌ Supabase error:', error.message)
        console.error('Error details:', JSON.stringify(error, null, 2))
      } else {
        console.log(`✅ ${batch.length} rows inserted`)
        lastFetchTime = now
        lastRowCount = batch.length
      }
    }

  } catch (err) {
    console.error('❌ Error:', err.message)
    console.error('Stack:', err.stack)
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

// Poll every 2 seconds
console.log('🚀 Starting Option Chain Poller')
console.log(`Token present: ${!!TOKEN}`)
console.log(`Supabase URL: ${process.env.SUPABASE_URL}`)

setInterval(fetchAndStoreOptionChain, POLL_INTERVAL)
fetchAndStoreOptionChain()  // Run immediately
```

Push this and check the logs. You should now see detailed output like:
```
📡 Fetching option chain...
Using expiry: 2026-02-27
API response status: success
Got 50 strikes
Spot price: 25700
Prepared 100 rows for insert
✅ 100 rows inserted
