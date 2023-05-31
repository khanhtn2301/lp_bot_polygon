#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================

# =============================================================================
# Imports
# =============================================================================
import os.path
import numpy as np
import pandas as pd
from pandas.api.indexers import FixedForwardWindowIndexer
from config import pools_v3_dict
from util import get_initial_liquidity

TICK = 1.0001


def extend_future_price_volatility(df, w_1=0.15, w_2=0.70, look_ahead_steps=500):
    """Calculates percentile prices in the future for emulating ML strategy.

    Args:
        df: The DataFrame containing p_current.
        w_1: The weight percentile for the first price cut p_1.
        w_2: The weight percentile for the second price cut p_2.
        look_ahead_steps: The size of the look ahead window.
    """

    df.loc[:, 'p_current_next'] = df['p_current']

    indexer = FixedForwardWindowIndexer(window_size=look_ahead_steps)

    df['p_min_next'] = df['p_current_next'].rolling(indexer, min_periods=1).min()
    df['p_1_next'] = df['p_current_next'].rolling(indexer, min_periods=1).quantile(w_1, interpolation='lower')
    df['p_2_next'] = df['p_current_next'].rolling(indexer, min_periods=1).quantile(w_2, interpolation='lower')
    df['p_max_next'] = df['p_current_next'].rolling(indexer, min_periods=1).max()

    return df


def get_liquidity_decimal(df, decimal0, decimal1):
    """derive liquidity decimal using tickUpper, tickLower, amount0 and amount1"""
    df['p_u'] = (TICK ** df['tickUpper']) * (10 ** (decimal0 - decimal1))
    df['p_l'] = (TICK ** df['tickLower']) * (10 ** (decimal0 - decimal1))
    df['liquidity'] = get_initial_liquidity(df['amount0'], df['amount1'], df['p_l'], df['p_u'])
    df['liq_decimal'] = np.round(np.log10(df['amount'] / df['liquidity']))
    decimal = df[['liq_decimal']].groupby('liq_decimal').agg(count=('liq_decimal', 'count')).reset_index() \
        .sort_values('count', ascending=False) \
        .head(1)['liq_decimal'].tolist()[0]
    return int(decimal)


def run(pools):
    for pool in pools:
        print("Processing pool {}/{} feeTier {}".format(
            pool['token0_symbol'],
            pool['token1_symbol'],
            pool['feeTier'],
        ))

        if not os.path.isfile('data/raw_data/uniswap_v3/swaps/{}{}_pool_fee_{}_swaps.csv'.format(
                pool['token0_symbol'],
                pool['token1_symbol'],
                pool['feeTier'])):
            continue

        decimal0 = pool['token0_decimals']
        decimal1 = pool['token1_decimals']

        def get_datas():
            datas = []
            for data_type in ['swaps', 'mints', 'burns']:
                data_name = 'data/raw_data/uniswap_v3/{}/{}{}_pool_fee_{}_{}.csv'.format(
                    data_type,
                    pool['token0_symbol'],
                    pool['token1_symbol'],
                    pool['feeTier'],
                    data_type)
                try:
                    data = pd.read_csv(data_name)
                    data['datetime'] = pd.to_datetime(data['datetime'])
                    data = data.set_index(pd.DatetimeIndex(data['datetime'].values))
                    datas.append(data)
                except FileNotFoundError:
                    print("File not found {}".format(data_name))
                    return None, None, None
            return datas

        swaps, mints, burns = get_datas()

        if swaps is None:
            continue

        # Liquidity amounts need to be adjusted by token decimals as it is a BigInteger in TheGraph
        liq_decimal = get_liquidity_decimal(mints, decimal0, decimal1)
        print(f'{pool["token0_symbol"]} decimal {decimal0} - {pool["token1_symbol"]} decimal {decimal1}')
        print(
            f'For pool {pool["token0_symbol"]}/{pool["token1_symbol"]} liquidity decimal is {liq_decimal}, double check!')

        mints['liquidity'] = mints['amount'] / (10 ** liq_decimal)
        burns['liquidity'] = - burns['amount'] / (10 ** liq_decimal)
        mints_burns = pd.concat([mints, burns], axis=0)
        mints_burns = mints_burns.sort_values(by='datetime')
        mints_burns = mints_burns[mints_burns['amount'] != 0]

        result_df = []
        print("Len of swaps {}".format(swaps.shape[0]))
        iter = 0
        for i, row in swaps.iterrows():
            iter += 1
            if iter % 1000 == 0:
                print("{} {}".format(iter, i))
            d_time = row['datetime']
            tick = int(row['tick'])
            m_b = mints_burns[mints_burns['datetime'] <= d_time]
            row['all_liquidity'] = m_b['liquidity'].sum()
            m_b_in_tick = m_b[(m_b['tickLower'] <= tick) & (tick <= m_b['tickUpper'])]
            row['total_liquidity_in_tick'] = m_b_in_tick['liquidity'].sum()
            result_df.append(row)
        result_df = pd.DataFrame(result_df)
        print(result_df.head())
        result_df.to_parquet("data/simulator/{}{}_pool_fee_{}.parquet".format(
            pool['token0_symbol'],
            pool['token1_symbol'],
            pool['feeTier']),
            compression=None
        )


if __name__ == '__main__':
    TOKEN0 = 'DAI'
    TOKEN1 = 'WETH'
    FEE_TIER = 3000
    pools_dict_sorted_arr = pools_v3_dict(token0=TOKEN0, token1=TOKEN1, fee_tier=FEE_TIER)
    _pools = pools_dict_sorted_arr[:200]
    run(_pools)
