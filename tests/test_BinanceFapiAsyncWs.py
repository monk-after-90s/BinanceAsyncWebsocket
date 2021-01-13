import asyncio as asyncio
from asyncUnittest import AsyncTestCase
from BinanceAsyncWebsocket import BinanceFapiAsyncWs
import ccxt.async_support as ccxt


class TestBinanceFapiAsyncWs(AsyncTestCase):
    @classmethod
    async def setUpClass(cls) -> None:
        apikey = input('Input apikey:')
        secret = input('Input secret:')

        cls.rest = ccxt.binance(
            {
                "apiKey": apikey,
                "secret": secret
            }
        )
        cls.ws = BinanceFapiAsyncWs(apikey)
        cls.msgs = []
        cls.st = None

        async def stream():
            cls.st = cls.ws.stream_filter()
            async for msg in cls.st:
                cls.msgs.append(msg)

        cls.stream_task = asyncio.create_task(stream())

    @classmethod
    async def tearDownClass(cls) -> None:
        ws_exit_task = asyncio.create_task(cls.ws.exit())
        rest_close_task = asyncio.create_task(cls.rest.close())
        st_exit_close_task = asyncio.create_task(cls.st.close())
        cls.stream_task.cancel()
        await ws_exit_task
        await rest_close_task
        await st_exit_close_task

    async def test_generate_listenkey(self):
        lk = await asyncio.create_task(self.ws._generate_listenkey(True))
        self.assertIn('listenKey', lk)

    async def test_open_cancel_order(self):
        new_order = await asyncio.create_task(
            self.rest.create_order(
                symbol='BTC/USDT', side='BUY', type='LIMIT', price=10, amount=0.001, params={'type': 'future'}))
        await asyncio.sleep(2)
        self.assertTrue(any(
            [(msg['e'] == "ORDER_TRADE_UPDATE" and msg['o']['x'] == 'NEW' and str(msg['o']['i']) == new_order['id']) for
             msg in self.msgs]))
        await asyncio.create_task(self.rest.cancel_order(new_order['id'], 'BTC/USDT', params={'type': 'future'}))
        await asyncio.sleep(2)
        self.assertTrue(any(
            [(msg['e'] == "ORDER_TRADE_UPDATE" and msg['o']['x'] == 'CANCELED' and str(msg['o']['i']) ==
              new_order['id']) for msg in self.msgs]))
