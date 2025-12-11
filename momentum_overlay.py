import pandas as pd
import numpy as np
from xbbg import blp
import blpapi
from scipy.stats import norm
from itertools import product

def fetch_futures_data(ticker, start_date, end_date, field='PX_LAST'):
    df = blp.bdh(tickers=ticker, flds=field,
                 start_date=start_date, end_date=end_date)
    df.index = pd.to_datetime(df.index)
    if isinstance(df.columns, pd.MultiIndex):
        df = df.loc[:, (slice(None), field)]
        df.columns = df.columns.droplevel(1)
    return df

def black_scholes_option(S, K, r, sigma, T, option_type='call'):
    if T <= 0 or sigma <= 0:
        return max(0.0, S - K) if option_type == 'call' else max(0.0, K - S)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'call':
        return S * norm.cdf(d1) - K * np.exp(-r*T) * norm.cdf(d2)
    else:
        return K * np.exp(-r*T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def performance_report(series):
    pnl = series
    cum = pnl.cumsum()
    total = float(cum.iloc[-1])
    mean = float(pnl.mean())
    std = float(pnl.std())
    sharpe = mean/std if std > 0 else np.nan
    dd = float((cum.cummax() - cum).max())
    return {
        "total": total,
        "mean": mean,
        "std": std,
        "sharpe": sharpe,
        "max_dd": dd,
    }

def futures_momentum(
        fut_ticker,
        start,
        end,
        target_daily_risk,
        max_contracts,
        signal_lookback,
        vol_lookback,
        signal_threshold,
):
    fut = fetch_futures_data(fut_ticker, start, end)
    pcol = fut.columns[0]
    fut['ret'] = fut[pcol].pct_change()
    fut['mom'] = fut[pcol].pct_change(signal_lookback)
    fut['vol'] = fut['ret'].rolling(vol_lookback).std()
    fut['signal_raw'] = fut['mom'] / fut['vol']
    fut['signal'] = fut['signal_raw'].replace([np.inf, -np.inf], np.nan)

    pnl, contracts_hist = [], []
    start_idx = max(signal_lookback, vol_lookback) + 1

    for i in range(start_idx, len(fut)):
        price = fut[pcol].iloc[i]
        prev_price = fut[pcol].iloc[i-1]
        sig = fut['signal'].iloc[i]

        if pd.isna(sig) or abs(sig) < signal_threshold:
            pnl.append(0.0)
            contracts_hist.append(0)
            continue

        daily_vol = fut['vol'].iloc[i]
        if pd.isna(daily_vol) or daily_vol <= 0:
            daily_vol = fut['vol'].dropna().iloc[-1]
        est_pnl_per_ctrt = price * daily_vol
        contracts = max(1, int(target_daily_risk / est_pnl_per_ctrt))
        contracts = min(contracts, max_contracts)

        direction = 1 if sig > 0 else -1
        fut_pnl = direction * (price - prev_price) * contracts

        pnl.append(fut_pnl)
        contracts_hist.append(direction * contracts)

    res = fut.iloc[start_idx:].copy()
    res['fut_pnl'] = pnl
    res['fut_contracts'] = contracts_hist
    return res

def add_option_overlay(
        base_df,
        risk_free_rate,
        ann_vol_assumption,
        option_days_to_expiry,
        call_moneyness,
        put_moneyness,
):
    pcol = base_df.columns[0]
    prices = base_df[pcol]
    dt = 1/252
    sigma = ann_vol_assumption

    opt_pos = 0
    opt_type = None
    K = None
    T = 0.0
    opt_val_prev = 0.0

    opt_pnl_list, opt_val_list, opt_type_list, K_list = [], [], [], []

    for i in range(len(base_df)):
        price = prices.iloc[i]
        contracts = base_df['fut_contracts'].iloc[i]

        if contracts == 0:
            opt_pnl = -opt_val_prev
            opt_pos = 0
            opt_type = None
            K = None
            T = option_days_to_expiry / 252.0
            opt_val = 0.0
        else:
            if opt_pos == 0:
                direction = 1 if contracts > 0 else -1
                if direction < 0:
                    opt_type = 'put'
                    K = round(price * put_moneyness)
                    opt_pos = 1
                else:
                    opt_type = 'call'
                    K = round(price * call_moneyness)
                    opt_pos = -1
                T = option_days_to_expiry / 252.0
                opt_val = black_scholes_option(price, K, risk_free_rate, sigma, T, opt_type)
                opt_pnl = 0.0
            else:
                T = max(T - dt, 0.0)
                opt_val = black_scholes_option(price, K, risk_free_rate, sigma, T, opt_type)
                opt_pnl = (opt_val - opt_val_prev) * opt_pos
                if T <= 0:
                    opt_pos = 0
                    opt_type = None
                    K = None

        opt_val_prev = opt_val
        scale = abs(base_df['fut_contracts'].iloc[i] or 1)
        opt_pnl_list.append(opt_pnl * scale)
        opt_val_list.append(opt_val)
        opt_type_list.append(opt_type if opt_type else 'none')
        K_list.append(K if K else np.nan)

    df = base_df.copy()
    df['opt_pnl'] = opt_pnl_list
    df['opt_value'] = opt_val_list
    df['opt_type'] = opt_type_list
    df['opt_strike'] = K_list
    df['strategy_pnl'] = df['fut_pnl'] + df['opt_pnl']
    return df

def grid_search(
        ticker,
        start,
        end,
        target_daily_risk,
        max_contracts,
        signal_lookback,
        vol_lookback,
        signal_thresholds,
        option_tenors,
        call_moneyness_list,
        put_moneyness_list,
        risk_free_rate,
        ann_vol_assumption,
):
    results = []

    for sig_th, tenor, c_mny, p_mny in product(
            signal_thresholds, option_tenors,
            call_moneyness_list, put_moneyness_list):

        base = futures_momentum(
            fut_ticker=ticker,
            start=start,
            end=end,
            target_daily_risk=target_daily_risk,
            max_contracts=max_contracts,
            signal_lookback=signal_lookback,
            vol_lookback=vol_lookback,
            signal_threshold=sig_th,
        )
        fut_stats = performance_report(base['fut_pnl'])

        with_opt = add_option_overlay(
            base_df=base,
            risk_free_rate=risk_free_rate,
            ann_vol_assumption=ann_vol_assumption,
            option_days_to_expiry=tenor,
            call_moneyness=c_mny,
            put_moneyness=p_mny,
        )
        strat_stats = performance_report(with_opt['strategy_pnl'])

        results.append({
            "signal_threshold": sig_th,
            "tenor_days": tenor,
            "call_moneyness": c_mny,
            "put_moneyness": p_mny,
            "strat_sharpe": strat_stats["sharpe"],
            "strat_max_dd": strat_stats["max_dd"],
        })

    res_df = pd.DataFrame(results)
    res_df = res_df.sort_values(by="strat_sharpe", ascending=False)
    return res_df