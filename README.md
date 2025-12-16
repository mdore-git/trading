# Trading – Bloomberg Python Scripts

This repository contains personal trading tools written in Python and designed to run from VS Code connected to a local Bloomberg Terminal via the Desktop API (blpapi). The focus is fixed income analytics, bond pair trading, and futures backtesting for professional use.[memory:2][memory:11]

## Environment

- OS: Windows with Bloomberg Terminal installed and logged in (Desktop API enabled).
- Python: 3.8–3.12 (tested on 3.12).
- IDE: VS Code with Python extension and Git integration enabled.

Pip instal pyyaml
Pip install pyarrow
Pip install polars_bloomberg

## Scripts

The repository may contain several independent scripts; all assume an active Bloomberg session:

- `FBES-PAIR-Copy-2.xlsx`  
  This Excel contains ISINs for filtered Spanish Financial Bonds with maturities >5Y used for the Pair Trading file.

- `Pair_Trading.py`  
  End‑to‑end Bloomberg‑driven corporate bond pairs trading system that builds a bond universe, tests cointegration, backtests Z‑score strategies, sizes trades, constructs a portfolio, and outputs detailed risk/performance analytics. 

- `auction_backtest.py`  
  Defines run_auction_backtest, which pulls OAT futures prices from Bloomberg and computes P&L per contract for all buy/sell day combinations around each auction date.

- `run_tests.py`  
  Runs the OAT auction backtest for 2025 auctions and prints pivot tables of average, volatility, and max loss P&L per contract by buy/sell timing.

- `momentum_overlay.py`  
  Implements a Bloomberg‑driven futures momentum strategy with dynamic position sizing, Black–Scholes option overlay, performance stats, and a grid search over signal and option parameters.

- `run_momentum_overlay.py`  
  Runs the TY1 futures momentum plus option overlay strategy with chosen parameters, prints P&L diagnostics, and performs a grid search to report the best Sharpe‑ratio configurations.

Each script has its own parameters (tickers, universe filters, dates, notional) defined at the top of the file so they can be quickly adjusted for new instruments or strategies.

## Notes

- All code is intended for use on a licensed Bloomberg Terminal; redistribution or bulk export of Bloomberg data may be restricted by Bloomberg’s terms of service.[memory:23]
- These tools are prototypes for trading-desk style analytics and are not production trading systems or investment advice.
