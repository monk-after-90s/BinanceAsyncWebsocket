import asyncio as asyncio
from asyncUnittest import AsyncTestCase
from BinanceAsyncWebsocket import BinanceFapiAsyncWs


class TestBinanceFapiAsyncWs(AsyncTestCase):
    @classmethod
    async def setUpClass(cls) -> None:
        # cls.ws = BinanceFapiAsyncWs(input('Input apikey:'))
        cls.ws = BinanceFapiAsyncWs('z9J4GCAR26SqfaHhiOIeB1b9LRPxdgrWwClFjyi2rcBmHMRyeUFQ9o6aanAnvmfx')

    @classmethod
    async def tearDownClass(cls) -> None:
        await asyncio.create_task(cls.ws.exit())

    async def test_generate_listenkey(self):
        lk = await asyncio.create_task(self.ws._generate_listenkey(True))
        self.assertIn('listenKey', lk)
