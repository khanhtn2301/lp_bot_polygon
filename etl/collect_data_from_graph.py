#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# =============================================================================
# Imports
# =============================================================================
import json
import os
import sys
import datetime
import time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pandas as pd
import requests
import numpy as np
from config import *

UNISWAP_VERSION = 'V3'
DATA_TYPE = 'burns'
START_DATE = "1/1/2021"
FETCH_LATEST = True

class UniswapGraphClient(object):
    """TheGraph client for Uniswap subgraphs."""

    def __init__(self, url):
        self._url = url

    def query(self, query: str, variables=None) -> dict:
        """Make graphql query to subgraph"""
        if variables:
            params = {'query': query, 'variables': variables}
        else:
            params = {'query': query}
        response_ = requests.post(self._url, json=params)
        response = response_.json()
        return response


class TheGraphClient(object):
    """TheGraph client for querying subgraph"""

    def __init__(self, url):
        self._url = url

    def query(self, query: str, variables=None) -> dict:
        """Make graphql query to subgraph"""
        if variables:
            params = {'query': query, 'variables': variables}
        else:
            params = {'query': query}
        response_ = requests.post(self._url, json=params)
        response = response_.json()
        return response

class UniswapV3PolygonData(UniswapGraphClient):

    def __init__(self, url=UNISWAP_V3_POLYGON_ENDPOINT):
        super().__init__(url)

    def get_pools(self):
        """Get latest factory data."""
        query = """
        query allPools($timestamp_start: BigInt!) {
          liquidityPools(
            first: 1000
            orderBy: createdTimestamp
            orderDirection: asc
            where: { createdTimestamp_gte: $timestamp_start }
          ){
            id
            createdTimestamp
            cumulativeVolumeUSD
            name
            fees(
                where: { feeType: FIXED_TRADING_FEE }
                ) {
                feeType
                feePercentage
                id
            }
            inputTokens {
                id
                name
                symbol
                decimals
                lastPriceUSD
                }
            }
        }
        """

        has_data = True
        all_data = []
        skip = 0
        variables = {
            'timestamp_start': 1620157950,
        }
        while has_data:
            data_response_ = self.query(query, variables)
            data_response = data_response_['data']['liquidityPools']
            print("got data response with len {}".format(len(data_response)))
            if len(data_response) == 0:
                break
            flattened_data = []
            for data_el in data_response:
                data_dict = {
                    'id': data_el['id'],
                    'feeTier': data_el['fees'][0]['feePercentage'],
                    'createdAtTimestamp': data_el['createdTimestamp'],
                    'token0_id': data_el['inputTokens'][0]['id'],
                    'token0_name': data_el['inputTokens'][0]['name'],
                    'token0_symbol': data_el['inputTokens'][0]['symbol'],
                    'token0_volumeUSD': data_el['inputTokens'][0]['lastPriceUSD'],
                    'token0_decimals': data_el['inputTokens'][0]['decimals'],
                    'token1_id': data_el['inputTokens'][1]['id'],
                    'token1_name': data_el['inputTokens'][1]['name'],
                    'token1_symbol': data_el['inputTokens'][1]['symbol'],
                    'token1_volumeUSD': data_el['inputTokens'][1]['lastPriceUSD'],
                    'token1_decimals': data_el['inputTokens'][1]['decimals'],
                    'volumeUSD': data_el['cumulativeVolumeUSD']
                }
                flattened_data.append(data_dict)
            all_data.extend(flattened_data)
            timestamps = set([int(data_el['createdTimestamp']) for data_el in data_response])
            variables['timestamp_start'] = max(timestamps)

            if len(data_response) < 1000:
                has_data = False

        if len(all_data) == 0:
            return None
        
        feeTiermapping = {
                         0.01: 100,
                         0.05: 500,
                         0.3: 3000,
                         1: 10000
                        }

        df = pd.DataFrame(all_data)
        df['feeTier'] = df['feeTier'].replace(feeTiermapping)
        df.drop_duplicates(inplace=True)

        return df

    def get_historical_pool_data(self, pool_address, data_type, query_str, data_dict_fn, time_delta=None,
                                 timestamp_start=None):
        """Collects historical pool data.

        Args:
            pool_address: The address of the pool.
            data_type: One of 'swaps', 'mints', 'collects' or 'burns'.
            query_str: The GQL query to the TheGraph.
            data_dict_fn: The dict structure of the response we want to use in the result `DataFrame`.
            time_delta: Time delta object to start the collection from.
            timestamp_start: Timestamp to start the collection from.

        Returns:
            df: The response parsed into a `DataFrame` using data_dict_fn.
        """

        if timestamp_start is not None:
            variables = {
                'id': pool_address,
                "timestamp_start": int(timestamp_start)
            }
        else:
            variables = {
                'id': pool_address,
                "timestamp_start": int((datetime.datetime.utcnow() - time_delta).replace(
                    tzinfo=datetime.timezone.utc).timestamp())
            }
        has_data = True
        all_data = []
        mapping_data_type = {'swaps':'swaps', 'mints':'withdraws', 'burns':'deposits'}
        try:
            while has_data:
                data_response_ = self.query(query_str, variables)
                # print(data_response_)
                data_response = data_response_['data']['liquidityPool'][mapping_data_type[data_type]]
                print("got data response with len {}".format(len(data_response)))

                if len(data_response) == 0:
                    break
                flattened_data = []
                for data_el in data_response:
                    data_dict = data_dict_fn(data_el)
                    flattened_data.append(data_dict)
                all_data.extend(flattened_data)
                timestamps = set([int(data_el['timestamp']) for data_el in data_response])
                variables['timestamp_start'] = max(timestamps)

                if len(data_response) < 1000:
                    has_data = False

                time.sleep(2)
        except:
            print('Error retrieving, process the acquired data')

        if len(all_data) == 0:
            print('Error! does not have any data')
            return None

        df = pd.DataFrame(all_data)
        df.timestamp = df.timestamp.astype(np.int64)
        df['datetime'] = pd.to_datetime(df.timestamp, unit='s')
        df.drop_duplicates(inplace=True)
        if 'amount0' in df.columns and 'amount1' in df.columns:
            df['priceInToken1'] = abs(df.amount1 / df.amount0)

        return df

def run_v3_all_pools(client):

    df = client.get_pools()
    if df is not None and not df.empty:
        print(df.head())
        df.to_csv("data/raw_data/uniswap_v3/pools.csv")


def run_v3(client, pools, data_type, fetch_latest=False):
    for pool in pools:
        print("processing pool {}/{} fee_tier {} with address {}".format(
            pool['token0_symbol'],
            pool['token1_symbol'],
            pool['feeTier'],
            pool['id']))
        df_base = pd.DataFrame()
        timestamp_start = datetime.datetime.strptime(START_DATE, "%d/%m/%Y").timestamp()
        if os.path.isfile("data/raw_data/uniswap_v3/{}/{}{}_pool_fee_{}_{}.csv".format(
                data_type,
                pool['token0_symbol'],
                pool['token1_symbol'],
                pool['feeTier'],
                data_type)):
            if fetch_latest:
                df_base = pd.read_csv("data/raw_data/uniswap_v3/{}/{}{}_pool_fee_{}_{}.csv".format(
                    data_type,
                    pool['token0_symbol'],
                    pool['token1_symbol'],
                    pool['feeTier'],
                    data_type))
                if 'Unnamed: 0' in df_base.columns:
                    df_base = df_base.drop(['Unnamed: 0'], axis=1)
                df_base.timestamp = df_base.timestamp.astype(np.int64)
                df_base['datetime'] = pd.to_datetime(df_base.timestamp, unit='s')
                timestamp_start = df_base['timestamp'].max()
                print("Fetch latest, shape {}".format(df_base.shape))
                print("base start {} end {}".format(df_base['timestamp'].min(), df_base['timestamp'].max()))
            else:
                print("already processed")
                continue
        pool_address = pool['id']
        if data_type == "swaps":
            df = client.get_historical_pool_data(pool_address=pool_address,
                                                 data_type="swaps",
                                                 query_str=THEGRAPH_QUERY_UNI_V3_POLY_SWAPS,
                                                 data_dict_fn=swaps_v3_polygon_dict,
                                                 timestamp_start=timestamp_start)
            df_token0_in = df[df['tokenIn'] == pool['token0_symbol']]
            df_token0_out = df[df['tokenOut'] == pool['token0_symbol']]
            replace_In_to_0 = {'tokenIn': 'token0', 'tokenIn_symbol': 'token0_symbol', 'amountIn': 'amount0', 'tokenIn_lastPriceUSD': 'token0_lastPriceUSD'}
            replace_Out_to_1 = {'tokenOut': 'token1', 'tokenOut_symbol': 'token1_symbol', 'tokenOut': 'amount1', 'tokenOut_lastPriceUSD': 'token1_lastPriceUSD'}
            df_token0_in = df_token0_in.rename(columns=replace_In_to_0 + replace_Out_to_1)
            df_token0_in[list(replace_Out_to_1.values())[:-1]] = df_token0_in[list(replace_Out_to_1.values())[:-1]] * -1

            replace_In_to_1 = {'tokenIn': 'token1', 'tokenIn_symbol': 'token1_symbol', 'amountIn': 'amount1', 'tokenIn_lastPriceUSD': 'token1_lastPriceUSD'}
            replace_Out_to_0 = {'tokenOut': 'token0', 'tokenOut_symbol': 'token0_symbol', 'tokenOut': 'amount0', 'tokenOut_lastPriceUSD': 'token0_lastPriceUSD'}
            df_token0_out = df_token0_out.rename(columns=replace_In_to_1 + replace_Out_to_0)
            df_token0_out[list(replace_Out_to_0.values())[:-1]] = df_token0_out[list(replace_Out_to_0.values())[:-1]] * -1

            df = pd.concat([df_token0_in, df_token0_out], axis=0).sort_values('timestamp')


        elif data_type == "mints":
            df = client.get_historical_pool_data(pool_address=pool_address,
                                                 data_type="mints",
                                                 query_str=THEGRAPH_QUERY_UNI_V3_POLY_MINTS,
                                                 data_dict_fn=mints_v3_polygon_dict,
                                                 timestamp_start=timestamp_start)
        elif data_type == "burns":
            df = client.get_historical_pool_data(pool_address=pool_address,
                                                 data_type="burns",
                                                 query_str=THEGRAPH_QUERY_UNI_V3_POLY_BURNS,
                                                 data_dict_fn=burns_v3_polygon_dict,
                                                 timestamp_start=timestamp_start)
        else:
            raise ValueError("Unsupported data_type {}".format(data_type))

        if df is not None and not df.empty:
            print(df.head())
            print("Fetched shape {}".format(df.shape))
            print("Fetched start {} end {}".format(df['timestamp'].min(), df['timestamp'].max()))
            if not df_base.empty:
                df = df[df['timestamp'] > timestamp_start]
                print("removing timestamp start: shape {}".format(df.shape))
                df = pd.concat([df_base, df], axis=0)
                print("After concat {}".format(df.shape))

            dirname = f"data/raw_data/uniswap_v3/{data_type}"
            if not os.path.exists(dirname):
                os.makedirs(dirname)

            #dedup data
            df = df.drop_duplicates(subset=['id'])
            df.to_csv("data/raw_data/uniswap_v3/{}/{}{}_pool_fee_{}_{}.csv".format(
                    data_type,
                    pool['token0_symbol'],
                    pool['token1_symbol'],
                    pool['feeTier'],
                    data_type)
            )


if __name__ == '__main__':
    UNISWAP_VERSION = 'V3'
    # 'swaps', 'mints', 'burns'
    DATA_TYPE = 'burns'
    FETCH_LATEST = False
    TOKEN0 = 'WETH'
    TOKEN1 = 'oSQTH'
    # FEE_TIER = 3000
    # print(f'process type {DATA_TYPE} for pair {TOKEN0}/{TOKEN1}')

    # pools_dict_arr = pools_v3_dict(token0=TOKEN0, token1=TOKEN1, fee_tier=FEE_TIER)
    # _pools = pools_dict_arr[:200]
    # graph_client = UniswapV3PolygonData()
    # run this to get pool data
    # run_v3_all_pools(client=graph_client)
    # run this to get data
    # run_v3(client=graph_client, pools=_pools, data_type=DATA_TYPE, fetch_latest=FETCH_LATEST)

