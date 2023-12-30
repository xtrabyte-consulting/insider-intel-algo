# Algotrading and Finance Models

## Basics of Algotrading

### Four Steps

Inputs: Stock Prices, Outside Info

1. Pre-trade Analysis: Alpha Models, Risk Model, Transaction Cost Model
2. Trading Signal: Portfolio Construction Model
3. Trade Execution: Execution Model
4. Post-Trade Anlysis

### Market Making

- Stacking Bids and Asks from order book
- Creating price based on stack

Time in force:

- Day order - valid for a day,
- Good Till Cancelled - valid until executed or cancelled

Conditional Orders:

- Stop Order - sell/buy when the price of a security falls/rises to a designated level
- Stop Limit Order - executed at the exact price once conditions are met (Become a limit order onece price is met)

Discretionary Order:

- Broker decides when to trade and the price

### Market Making Example

- Naural Price = Last Price - Relative Index Chamge * Std. Dev.
- (last price) - (index now - index at last price) * (price std. dev.)
- If natural price > actual price: Buy

- Use data to create buy/sell signal
- Execute on signal

### Steps in Building an Algo

1. Define trading hypothesis and goal (codable rule)
2. Set operating time horizon and constraints (programmable start/stop rules)
3. Algo testing (keep algo's level of risk within reason)

### Maintaining Algorithm

- Continual monitoring and maintenance: performance, market conditions
- Mainenence and rejuvenation: All strategies degrade

### Requirements

- Centralized order book
- Access to high-frequency, liquid markets
- In-house systems
- Client systems
- Vendor systems
- Information exchane

### Algo Example

- Boeing vs Triumph (landing gear) are indicators

### Text data

- Computation linguistics
- Qualitative data -> quantitative
- Sources: EDGAR, WSJ, Audio transcripts from Seeking Alpha, Twitter/StockTwits
- Programs: WordStat, Lexalystics, Diction
- Components: Download and convert.

### Algo Trading as Career

- Finance/CS guy paired in teamss to conceptualize theories and then program tools/strategies
