const WebSocket = require('ws')
const _ = require('lodash')
const { cryptoLib } = require('../crypto/cryptoLib')
const { ftx } = require('../crypto/ftx')
const { binance } = require('../crypto/binance')
const { okex } = require('../crypto/okex')
const { telegram } = require('../telegram/bot')
const { getStandarPriceFormat } = require('./common')

/**
 * This program will not run out of the box, is just a sample code that I provided from my HFT bots
 * 
 * How to look at this is to start from:
 * 1. hft ()                - will automatically open a websocket connection from every exchange mentioned in wsExchanges array
 * 2. wsInit()              - will add listeners around a ws for open, ping, error, message
 * 3. wsOpen[exxchange]()   - subscribing to each exchange to fetch prices
 * 4. wsMessage[exchange]() - index each price inside the global object price
 */
const PRECISION_SPREAD = 2
const CACHE_SOCKET_LOG = __dirname + '/cache/socket_log.json'
const wsExchanges = ['ftx', 'binance', 'okex']

let ws = {}, wsOpen = {}, wsMessage = {},  // ws objects
  rcv = {}, rcvTs = {}, // ws counter and last timestamp
  price = {} // prices are updated inside this object {'ftx': {'BTC':{b: 101, a: 102}}}
let orderId = {} // ws state for all the orders placed in the exchanges


//----------------
// WS OPEN
//----------------
wsOpen.ftx = async () => {
  ws.ftx.send(ftx.wsLoginOp())
  ws.ftx.send(JSON.stringify({ 'op': 'subscribe', 'channel': 'fills' }))
  const pairs = await ftx.getFuturesListFtx()
  pairs.map(el => {
    ws.ftx.send(JSON.stringify({ 'op': 'subscribe', 'channel': 'ticker', 'market': el, depth: 1 })),
      ws.ftx.send(JSON.stringify({ 'op': 'subscribe', 'channel': 'orderbook', 'market': el, depth: 1 }))
  })
  setInterval(() => { ws.ftx.ping() }, 14000)
}
wsOpen.binance = async () => {
  const pairs = await binance.getContractsPairs()
  ws.binance.send(binance.wsSubscribe(pairs, 'bookTicker'))
  const ping = () => { ws.binance.pong() }
  setInterval(ping, 3 * 1000 * 60)
}
wsOpen.okex = async () => {
  const pairs = await okex.getContractsPairs()
  pairs.map(m => ws.huobi.send(okex.wsSubscribeTicker(m)))
}

//------------------------
//  WS Message processing, for every recieved price message we update our price object with bid and ask
//------------------------
wsMessage.ftx = async (msg, e) => {
  const { type, channel, market: symbol, data } = JSON.parse(msg)
  if (!data || !type || !channel) { return }
  switch (channel) {
    case 'ticker': {
      const { bid, ask, bidSize, askSize, last, time } = data
      // skip old data regarding prices
      if (new Date().getTime() - time * 1000 > 400) { break }

      const pair = symbol
      if (!price[e]?.[pair]) {
        price[e][pair] = getStandarPriceFormat()
        orderId[pair] = { b: false, a: false, wa: false, wb: false, pb: false, pa: false }
      }
      price[e][pair] = {
        a: ask, b: bid,
        s: _.round((price[e][pair].a - price[e][pair].b) / price[e][pair].a, PRECISION_SPREAD),
        m: _.round((price[e][pair].a + price[e][pair].b) / 2, PRECISION_SPREAD),
        t: Math.ceil(time * 1000)
      }
      break
    }
    default:
      console.log('[ftx][default_message]', json)
  }
}
wsMessage.binance = async (msg, ex) => {
  const { e, T, s, b, a, B, A, result } = JSON.parse(msg)
  
  const pair = binance.getStandardPair(s)
  if (!price[ex]?.[pair]) { price[ex][pair] = getStandarPriceFormat() }
  price[ex][pair].b = parseFloat(b)
  price.binance[pair].a = parseFloat(a)
  price.binance[pair].s = _.round((price[ex][pair].a - price[ex][pair].b) / price[ex][pair].a * 100, PRECISION_SPREAD)
  price.binance[pair].m = _.round((price[ex][pair].a + price[ex][pair].b) / 2, PRECISION_SPREAD)
  price.binance[pair].t = T
}
wsMessage.okex = async (msg, e) => {
  const { event, args, data } = JSON.parse(msg)

  if (data?.[0]) {
    const { instId, askPx, bidPx, ts } = data[0]
    const pair = okex.getStandardPair(instId)

    if (!price[e]?.[pair]) { price[e][pair] = getStandarPriceFormat() }
    price[e][pair].b = parseFloat(bidPx)
    price[e][pair].a = parseFloat(askPx)
    price[e][pair].s = _.round((price[e][pair].a - price[e][pair].b) / price[e][pair].a * 100, 3)
    price[e][pair].m = _.round((price[e][pair].a + price[e][pair].b) / 2, PRECISION_SPREAD)
    price[e][pair].t = parseInt(ts)
  }
}

//----------------
// WS initializer with default methods for open, ping, error, message
//----------------
const wsInit = (ws) => {
  ws.on('open', async function open() {
    price[ws.exchange] = []
    rcv[ws.exchange] = 0
    rcvTs[ws.exchange] = 0
    wsOpen[ws.exchange]()
    console.log(`[${ws.exchange}] Open connection`)
  })
  ws.on('ping', (e) => {
    cryptoLib.write(CACHE_SOCKET_LOG, { log: `[${ws.exchange}][PING_FRAME][${new Date().getTime()}]`, err: JSON.stringify(e) })
    ws.pong()
  })
  ws.on('error', (err) => {
    console.log(`[${ws.exchange}][ws_error]`, err)
    const msg = `${ws.exchange} Error`
    telegram.sendMsg(msg + JSON.stringify(err))
    cryptoLib.write(CACHE_SOCKET_LOG, { log: `[${ws.exchange}][ws_error]`, err: JSON.stringify(err) })
    process.exit(0)
  })
  ws.on('message', async (msg) => {
    rcv[ws.exchange]++
    rcvTs[ws.exchange] = new Date().getTime()
    wsMessage[ws.exchange](msg, ws.exchange)
  })
}

//----------------
// HFT bot initializer, it will open connection for every exchange in the array wsExchanges
//----------------
const hft = async () => {
  wsExchanges.map(async e => {
    let wsUrl = eval(e).WS_URL
    ws[e] = new WebSocket(wsUrl)
    ws[e].exchange = e
    wsInit(ws[e])
    orderId[e] = {}
    console.log(`[${e}] ${wsUrl}`)
  })
}

// Alert if there are any orders left in the queue, in case this program crashes
process.on('SIGINT', async function () {
  console.log('Caught interrupt signal', orderId)
  telegram.sendMsg(msg + JSON.stringify(err))
  process.exit(0)
})

hft()
