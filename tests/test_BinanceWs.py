import asyncio

import beeprint
from loguru import logger

from BinanceAsyncWebsocket import BinanceWs
import asyncUnittest
from asyncUnittest import AsyncTestCase
import ccxt.async_support as ccxt
import os
import json

if os.path.exists(os.path.join(os.path.dirname(__file__), 'key_secret.json')):
    with open('key_secret.json') as f:
        key_secret = json.load(f)
        test_apikey = key_secret['apikey']
        test_secret = key_secret['secret']
else:
    test_apikey = input('Test apikey，或者在同文件夹下放入文件key_secret.json,包含apikey和secret字典:')
    test_secret = input('Test secret:')


class TestOrder(AsyncTestCase):
    enable_test = 1
    aws: BinanceWs = None
    bn: ccxt.binance = None

    @classmethod
    async def setUpClass(cls):
        cls.aws = BinanceWs(test_apikey)
        cls.bn = ccxt.binance({
            "apiKey": test_apikey,
            "secret": test_secret, }
        )

    @classmethod
    async def tearDownClass(cls) -> None:
        bn_exit_task = asyncio.create_task(cls.bn.close())
        aws_exit_task = asyncio.create_task(cls.aws.exit())
        try:
            await bn_exit_task
        except:
            pass
        try:
            await aws_exit_task
        except:
            pass

    async def test_huge_order_messages(self):
        '''
        超量订单信息测试

        :return:
        '''
        order_num = 10
        price = int((await type(self).bn.fetch_order_book('BTC/USDT'))['bids'][0][0] * 0.8)

        async def open_orders():
            await asyncio.sleep(5)
            [asyncio.create_task(type(self).bn.create_order('BTC/USDT', 'limit', 'buy', 0.001, price))
             for _ in range(order_num)]

        asyncio.create_task(open_orders())

        all_order_stream = type(self).aws.order_stream()
        n = 0
        ids = set()
        async for msg in all_order_stream:
            if msg['x'] == 'NEW' and float(msg['p']) == price and msg['o'] == 'LIMIT' and msg['s'] == "BTCUSDT" and \
                    float(msg['q']) == 0.001:
                ids.add(msg['i'])
                n += 1
                if n >= order_num:
                    break
        [asyncio.create_task(type(self).bn.cancel_order(order_id, 'BTC/USDT'))
         for order_id in ids]
        async for msg in all_order_stream:
            if msg['x'] == 'CANCELED' and float(msg['p']) == price and msg['o'] == 'LIMIT' and msg['s'] == "BTCUSDT" and \
                    float(msg['q']) == 0.001:
                n -= 1
                if n <= 0:
                    break
        await all_order_stream.close()


if __name__ == '__main__':
    asyncUnittest.run()
