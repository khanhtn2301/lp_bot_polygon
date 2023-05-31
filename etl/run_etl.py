#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================

# =============================================================================
# Imports
# =============================================================================
from collect_data_from_graph import *
from config import pools_v3_dict

if __name__ == '__main__':
    DATA_TYPES = ['swaps', 'mints', 'burns']
    FETCH_LATEST = True
    # TOKEN0 = 'WBTC'
    # TOKEN1 = 'WETH'
    # FEE_TIER = 3000
    pools = [
        # ('WBTC', 'WETH', 500),
        # ('WETH', 'USDT', 500),
        # ('DAI', 'USDC', 500),
        # ('USDC', 'USDT', 100),
        # ('USDC', 'WETH', 500),
        # ('USDC', 'WETH', 500),
        # ('FRAX', 'USDC', 500),
        # ('WETH', 'CRV', 10000),
        # ('FTM', 'WETH', 10000),
        # ('MATIC', 'WETH', 3000),
        # ('WETH', 'USDT', 3000),
        ('WBTC', 'WETH', 500)
    ]

    for TOKEN0, TOKEN1, FEE_TIER in pools:
        pools_dict_arr = pools_v3_dict(token0=TOKEN0, token1=TOKEN1, fee_tier=FEE_TIER)
        _pools = pools_dict_arr[:200]
        graph_client = UniswapV3PolygonData()
        if True:
            for datatype in DATA_TYPES:
                print(f'process type {datatype} for pair {TOKEN0}/{TOKEN1}')
                run_v3(client=graph_client, pools=_pools, data_type=datatype, fetch_latest=FETCH_LATEST)
