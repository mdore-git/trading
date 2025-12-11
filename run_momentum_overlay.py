from momentum_overlay import (
    futures_momentum,
    add_option_overlay,
    performance_report,
    grid_search,
)

def main():
    ticker = "TY1 Comdty"
    start = "2020-01-01"
    end = "2025-11-30"

    target_daily_risk = 500.0
    max_contracts = 10
    signal_lookback = 20
    vol_lookback = 20
    signal_threshold = 0.5

    risk_free_rate = 0.03
    ann_vol_assumption = 0.06
    option_days_to_expiry = 30
    call_moneyness = 1.01      # 1.00 = ATM, 1.01 = 1% OTM
    put_moneyness = 1.00       # 1.00 = ATM, 0.99 = 1% ITM

    signal_thresholds = [0.3, 0.5, 0.7]
    option_tenors = [10, 30, 60]
    call_moneyness_list = [1.00, 1.01]
    put_moneyness_list = [1.00, 0.99]

    base = futures_momentum(
        fut_ticker=ticker,
        start=start,
        end=end,
        target_daily_risk=target_daily_risk,
        max_contracts=max_contracts,
        signal_lookback=signal_lookback,
        vol_lookback=vol_lookback,
        signal_threshold=signal_threshold,
    )
    base_stats = performance_report(base['fut_pnl'])

    with_opt = add_option_overlay(
        base_df=base,
        risk_free_rate=risk_free_rate,
        ann_vol_assumption=ann_vol_assumption,
        option_days_to_expiry=option_days_to_expiry,
        call_moneyness=call_moneyness,
        put_moneyness=put_moneyness,
    )
    strat_stats = performance_report(with_opt['strategy_pnl'])

    print(with_opt[['strategy_pnl', 'fut_pnl', 'opt_pnl', 'fut_contracts']].tail())
    print("Base futures-only total PnL:", round(base_stats["total"], 2))
    print("Base futures+options total PnL:", round(strat_stats["total"], 2))

    print("\nGrid search over thresholds / tenors / moneyness:")
    grid_res = grid_search(
        ticker=ticker,
        start=start,
        end=end,
        target_daily_risk=target_daily_risk,
        max_contracts=max_contracts,
        signal_lookback=signal_lookback,
        vol_lookback=vol_lookback,
        signal_thresholds=signal_thresholds,
        option_tenors=option_tenors,
        call_moneyness_list=call_moneyness_list,
        put_moneyness_list=put_moneyness_list,
        risk_free_rate=risk_free_rate,
        ann_vol_assumption=ann_vol_assumption,
    )
    print(grid_res.head(10))

if __name__ == "__main__":
    main()
