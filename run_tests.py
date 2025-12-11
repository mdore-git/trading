from auction_backtest import run_auction_backtest
import pandas as pd

auctions_2025 = [
    '2025-01-02','2025-02-06','2025-03-06','2025-04-03','2025-05-02',
    '2025-06-05','2025-07-03','2025-09-04','2025-10-02','2025-11-06',
]

oat_trades = run_auction_backtest('OAT1 Comdty', auctions_2025)

def print_matrix(title, table):
    print(f"\n=== {title} ===")
    print(table.round(1).to_string())

mean_tbl = oat_trades.pivot_table(
    index='buy_days_before',
    columns='sell_days_after',
    values='pnl_per_contract',
    aggfunc='mean'
)
print_matrix("Avg P&L per contract (EUR)", mean_tbl)

std_tbl = oat_trades.pivot_table(
    index='buy_days_before',
    columns='sell_days_after',
    values='pnl_per_contract',
    aggfunc='std'
)
print_matrix("Std dev P&L per contract (EUR)", std_tbl)

min_tbl = oat_trades.pivot_table(
    index='buy_days_before',
    columns='sell_days_after',
    values='pnl_per_contract',
    aggfunc='min'
)
print_matrix("Max loss per contract (EUR)", min_tbl)
