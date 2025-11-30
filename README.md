# MEATNA - Multi-Exchange Arbitrage Trading Bot

A high-frequency arbitrage bot that finds and exploits price differences across cryptocurrency exchanges in real-time.

## What It Does

If Bitcoin costs $50,000 on Coinbase but $50,100 on Kraken. This bot finds those opportunities, calculates if the profit covers trading fees, and executes trades automatically. It monitors 1,454+ possible trading paths across multiple exchanges simultaneously.

## Key Features

### Real-Time Price Monitoring

- WebSocket connections to multiple exchanges (Coinbase, Kraken)
- Processes live orderbook updates as they happen
- No polling delays - instant price discovery

### Smart Arbitrage Detection

- Graph-based pathfinding algorithm
- Evaluates triangular and cross-exchange arbitrage opportunities
- Accounts for trading fees, slippage, and minimum order sizes
- Risk-adjusted profit calculations with volatility modeling

### Optimized Performance

The original version used sequential REST API calls to fetch prices. I rebuilt it with:

- **Concurrent WebSocket subscriptions** using asyncio
- **Parallel data fetching** with semaphore-based rate limiting
- **Result**: 98% faster initialization (100s -> 2s)

## How It Works

```
1. Connect to Exchanges
   - Establish WebSocket connections
   - Load market data for all trading pairs

2. Build Market Graph
   - Create nodes for each asset (BTC, ETH, USDC, etc.)
   - Create edges for each trading pair

3. Find Arbitrage Paths
   - Run DFS to discover profitable cycles
   - Calculate expected profit after fees
   - Filter by risk/volatility thresholds

4. Monitor Opportunities
   - Update orderbooks in real-time
   - Re-evaluate paths every 100ms
   - Execute trades when profitable
```

## Technical Architecture

### Core Components

**Market Graph** (market_graph.py)

- Directed graph where nodes = assets, edges = trading pairs
- Each market creates 2 edges: BUY (quote to base) and SELL (base to quote)

**Path Model** (path_model.py)

- DFS algorithm to find profitable trading cycles
- Filters by token rules and minimum path length
- Validates paths don't revisit nodes (prevents infinite loops)

**Path Evaluator** (path_evaluator.py)

- Simulates trade execution across orderbook depth
- Calculates VWAP (volume-weighted average price)
- Accounts for slippage, fees, and volatility

**Orderbook Cache** (orderbook_cache.py)

- Thread-safe cache for live orderbook snapshots
- Updates via WebSocket for low-latency data

**WebSocket Manager** (websocket_manager.py)

- Manages concurrent WebSocket connections
- Auto-reconnection on disconnects
- Processes 25-level orderbooks per market

### Data Flow

```
WebSocket Streams
    -> Orderbook Cache
    -> Path Evaluator
    -> Trading Engine

Volatility Cache provides risk adjustment to Path Evaluator
```

## Project Structure

```
MEATNA/
  main.py                    - Main entry point
  requirements.txt           - Python dependencies
  README.md                  - Project documentation

  config/
    config.yaml              - Bot configuration
    secrets.yaml             - API credentials

  meatna/
    core/
      arbitrage_scanner.py   - Main arbitrage scanner
      balance.py             - Account balance tracking
      config.py              - Configuration loader
      market_graph.py        - Graph construction
      orderbook_cache.py     - Real-time orderbook storage
      path_evaluator.py      - Profit simulation
      path_model.py          - Path finding (DFS)
      volatility_cache.py    - Risk metrics

    exchange/
      __init__.py            - Package initialization
      exchange_client.py     - Multi-exchange REST client
      models.py              - Data models
      websocket_manager.py   - WebSocket client

    infra/
      account_service.py     - Account management
      polling_manager.py     - REST polling fallback
      rest_bootstrap.py      - Bootstrap data loader

    utils/
      logging.py             - Debug utilities
      math_utils.py          - Statistical functions
```

## Performance Improvements

### Before: Sequential REST Polling

```python
# Old approach - 100+ seconds
for market in markets:
    orderbook = await exchange.fetch_orderbook(market)  # 1-2s each
    cache.update(orderbook)
```

### After: Concurrent WebSocket Streams

```python
# New approach - 2 seconds
async def subscribe_all():
    tasks = [
        watch_orderbook(market)  # Real-time updates
        for market in markets
    ]
    await asyncio.gather(*tasks)  # Parallel execution
```

Result: Went from 100s initialization to 2s, enabling true real-time arbitrage.

## Configuration

Edit `config/config.yaml`:

```yaml
paths:
  min_length: 2 # Minimum arbitrage path length
  max_length: 4 # Maximum path length
  allow_revisit_nodes: false # Prevent cycles

risk_model:
  min_profit_margin: 0.001 # 0.1% minimum profit
  vol_risk_multiplier: 0.5 # Volatility penalty
  slippage_coefficient: 0.00001

execution:
  max_concurrent_paths: 1 # Trades at once
  inter_leg_timeout_seconds: 1.0
```

## Setup

1. **Install dependencies**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure API keys**

Create `config/secrets.yaml`:

```yaml
coinbase:
  api_key: 'your_api_key'
  api_secret: 'your_secret'

kraken:
  api_key: 'your_api_key'
  api_secret: 'your_secret'
```

3. **Run the bot**

```bash
python main.py
```

## Example Output

```
Starting WebSocket subscriptions...
  - coinbase: 150 markets
  - kraken: 120 markets
  - WebSocket subscriptions active

Scan Results:
  - Evaluated: 1,454 paths
  - Opportunities: 3
  - Scan time: 12.5ms

Best Opportunity:
  Path: USDC -> BTC -> ETH -> USDC
  Expected profit: 0.18%
  After fees: 0.12%
```

## Why This Matters

Traditional arbitrage bots poll exchanges every few seconds, missing opportunities that exist for milliseconds. By switching to WebSockets and async processing:

- **Latency**: Sub-second price updates vs 1-5 second polling
- **Efficiency**: One connection per exchange vs hundreds of API calls
- **Scalability**: Can monitor 1,000+ markets without rate limits

This architecture makes the difference between catching arbitrage opportunities and watching them disappear.

## Tech Stack

- **Python 3.11+**: Async/await, type hints
- **CCXT Pro**: Exchange WebSocket integration
- **asyncio**: Concurrent task management
- **Graph algorithms**: Custom DFS implementation for cycle detection
