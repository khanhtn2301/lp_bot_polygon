#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# Imports
# =============================================================================
import pandas as pd
from decimal import Decimal
import os
from dotenv.main import load_dotenv

load_dotenv()


API_KEY_APC_ANKR = os.environ['API_KEY_APC_ANKR']
API_KEY_POLYGON_SCAN = os.environ['API_KEY_POLYGON_SCAN']
UNISWAP_CONTRACT_ADDRESS = os.environ['UNISWAP_CONTRACT_ADDRESS']

UNISWAP_V3_POLYGON_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-polygon'

W3_ENDPOINT = f'https://rpc.ankr.com/polygon/{API_KEY_APC_ANKR}'

ABI_UNISWAP_CONTRACT = os.environ['ABI_UNISWAP_CONTRACT']

ABI_UNISWAP_CONTRACT_ENDPOINT = f'https://api.polygonscan.com/api?module=contract&action=getabi&address={UNISWAP_CONTRACT_ADDRESS}&apikey={API_KEY_POLYGON_SCAN}'

def pools_v3_dict(token0=None, token1=None, fee_tier=None):
    pools_df = pd.read_csv('data/raw_data/uniswap_v3/pools.csv')
    pools_dict_arr = pools_df.to_dict(orient='records')
    if token0 is not None and token1 is not None and fee_tier is not None:
        pools_dict_arr = [dict_el for dict_el in pools_dict_arr if
                          dict_el['token0_symbol'] == token0 and dict_el['token1_symbol'] == token1 and
                          dict_el['feeTier'] == fee_tier]
    pools_dict_arr = sorted(pools_dict_arr, key=lambda x: float(x['volumeUSD']), reverse=True)

    return pools_dict_arr

THEGRAPH_QUERY_UNI_V3_POLY_SWAPS = """
            query swapsQuery($id: String!, $timestamp_start: Int!){
                liquidityPool( id: $id 
                ) {
                    swaps(
                    first: 1000
                    orderBy: timestamp
                    orderDirection: asc
                    where: { timestamp_gte: $timestamp_start }
                    ) {
                    id
                    timestamp
                    tokenIn {
                        id
                        symbol
                        name
                        decimals
                        lastPriceUSD
                    }
                    tokenOut {
                        id
                        symbol
                        name
                        decimals
                        lastPriceUSD
                    }
                    amountIn
                    amountOut
                    amountInUSD
                    tick
                    }
                }
            }
        """

def swaps_v3_polygon_dict(data_el):
    data_dict = {
        'id': data_el['id'],
        'timestamp': data_el['timestamp'],
        'tokenIn_id': data_el['tokenIn']['id'],
        'tokenIn_symbol': data_el['tokenIn']['symbol'],
        'tokenIn_lastPriceUSD': float(data_el['tokenIn']['lastPriceUSD']),
        'tokenOut_id': data_el['tokenOut']['id'],
        'tokenOut_symbol': data_el['tokenOut']['symbol'],
        'tokenOut_lastPriceUSD': float(data_el['tokenOut']['lastPriceUSD']),
        'amountIn': float(data_el['amountIn']),
        'amountOut': float(data_el['amountOut']),
        'amountUSD': float(data_el['amountInUSD']),
        'tick': int(data_el['tick']),
    }
    return data_dict

THEGRAPH_QUERY_UNI_V3_POLY_MINTS = """
            query mintsQuery($id: String!, $timestamp_start: Int!){
                liquidityPool(id: $id) {
                    withdraws(
                    first: 1000
                    orderBy: timestamp
                    orderDirection: asc
                    where: { timestamp_gte: $timestamp_start }
                    ) {
                    id
                    timestamp
                    hash
                    inputTokens {
                        id
                        symbol
                        name
                        decimals
                        lastPriceUSD
                    }
                    amountUSD
                    inputTokenAmounts
                    tickUpper
                    tickLower
                    account {
                            id
                        }
                    position{
                            id
                        }
                    }
                }
            }
        """

def mints_v3_polygon_dict(data_el):
    data_dict = {
        'id': data_el['id'],
        'hash': data_el['hash'],
        'owner': data_el['account']['id'],
        'timestamp': data_el['timestamp'],
        'token0_id': data_el['inputTokens'][0]['id'],
        'token0_symbol': data_el['inputTokens'][0]['symbol'],
        'token0_priceUSD': float(data_el['inputTokens'][0]['lastPriceUSD']),
        'token1_id': data_el['inputTokens'][1]['id'],
        'token1_symbol': data_el['inputTokens'][1]['symbol'],
        'token1_priceUSD': float(data_el['inputTokens'][1]['lastPriceUSD']),
        'amount0': float(data_el['inputTokenAmounts'][0]),
        'amount1': float(data_el['inputTokenAmounts'][1]),
        'amountUSD': float(data_el['amountUSD']),
        'tickLower': int(data_el['tickLower']),
        'tickUpper': int(data_el['tickUpper'])
    }

    return data_dict

THEGRAPH_QUERY_UNI_V3_POLY_BURNS = """
            query mintsQuery($id: String!, $timestamp_start: Int!){
                liquidityPool(id: $id) {
                    deposits(
                    first: 1000
                    orderBy: timestamp
                    orderDirection: asc
                    where: { timestamp_gte: $timestamp_start }
                    ) {
                    id
                    timestamp
                    hash
                    inputTokens {
                        id
                        symbol
                        name
                        lastPriceUSD
                    }
                    amountUSD
                    inputTokenAmounts
                    tickUpper
                    tickLower
                    account {
                            id
                        }
                    }
                }
            }
        """

def burns_v3_polygon_dict(data_el):
    data_dict = {
        'id': data_el['id'],
        'hash': data_el['hash'],
        'owner': data_el['account']['id'],
        'timestamp': data_el['timestamp'],
        'token0_id': data_el['inputTokens'][0]['id'],
        'token0_symbol': data_el['inputTokens'][0]['symbol'],
        'token0_priceUSD': float(data_el['inputTokens'][0]['lastPriceUSD']),
        'token1_id': data_el['inputTokens'][1]['id'],
        'token1_symbol': data_el['inputTokens'][1]['symbol'],
        'token1_priceUSD': float(data_el['inputTokens'][1]['lastPriceUSD']),
        'amount0': float(data_el['inputTokenAmounts'][0]),
        'amount1': float(data_el['inputTokenAmounts'][1]),
        'amountUSD': float(data_el['amountUSD']),
    }

    return data_dict
