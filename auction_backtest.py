import pandas as pd
from datetime import timedelta
from xbbg import blp

def run_auction_backtest(
    fut_ticker,
    auctions,
    par=100000.0,
    n_contracts=10,
    field_px='PX_MID',
    buy_days=(1, 2, 3),
    sell_days=(1, 2, 3),
):
    auctions = [pd.to_datetime(d) for d in auctions]
    combos = [(b, s) for b in buy_days for s in sell_days]
    rows = []

    for a_date in auctions:
        for b_days, s_days in combos:
            buy_date = a_date - timedelta(days=b_days)
            sell_date = a_date + timedelta(days=s_days)
            start_query = buy_date - timedelta(days=7)

            df_px = blp.bdh(
                tickers=fut_ticker,
                flds=field_px,
                start_date=start_query.strftime('%Y-%m-%d'),
                end_date=sell_date.strftime('%Y-%m-%d'),
            )
            if df_px.empty:
                continue

            df_px.index = pd.to_datetime(df_px.index)
            px_col = (fut_ticker, field_px)
            if px_col not in df_px.columns:
                for c in df_px.columns:
                    if str(c) == str(px_col) or (isinstance(c, tuple) and c[0] == fut_ticker):
                        px_col = c
                        break

            px_series = df_px[px_col].dropna()
            if len(px_series) < 2:
                continue

            buy_px = px_series[px_series.index <= buy_date].iloc[-1]
            sell_px = px_series[px_series.index <= sell_date].iloc[-1]

            pnl_per_ctrt = (sell_px - buy_px) * par / 100.0
            total_pnl = pnl_per_ctrt * n_contracts

            rows.append({
                'auction_date': a_date,
                'buy_days_before': b_days,
                'sell_days_after': s_days,
                'buy_date': buy_date,
                'sell_date': sell_date,
                'buy_px': buy_px,
                'sell_px': sell_px,
                'pnl_per_contract': pnl_per_ctrt,
                'pnl_eur': total_pnl,
            })

    return pd.DataFrame(rows)
