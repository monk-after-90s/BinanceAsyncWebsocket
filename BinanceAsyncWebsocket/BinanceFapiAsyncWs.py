import json

import aiohttp
from AsyncWebsocketStreamInterface import AsyncWebsocketStreamInterface
import asyncio
import websockets


class BinanceFapiAsyncWs(AsyncWebsocketStreamInterface):
    ws_baseurl = 'wss://fstream.binance.com'
    restful_baseurl = 'https://fapi.binance.com'

    def __init__(self, apikey):
        super(BinanceFapiAsyncWs, self).__init__()
        self._apikey = apikey
        self._session: aiohttp.ClientSession = None
        self._delay_listenKey_invalid_running = False

    @property
    def session(self):
        if not self._session:
            self._session = aiohttp.ClientSession()

        return self._session

    async def _generate_listenkey(self, debug=False):
        if not self._delay_listenKey_invalid_running:  # 确保只运行一个心跳
            asyncio.create_task(self._delay_listenKey_invalid())
            self._delay_listenKey_invalid_running = True

        async with self.session.post(
                self.restful_baseurl + '/fapi/v1/listenKey',
                headers={'X-MBX-APIKEY': self._apikey},
                # data={
                #     'recvWindow': 5000,
                #     'timestamp': ts,
                #     'signature': self._generate_signature(recvWindow=5000, timestamp=ts)}
        ) as r:
            if not debug:
                listenKey = (await r.json())['listenKey']
                return listenKey
            else:
                return await r.json()

    async def _delay_listenKey_invalid(self):
        while True:
            await asyncio.create_task(asyncio.sleep(30 * 60))
            await self._generate_listenkey()

    async def _create_ws(self):
        ws = await websockets.connect(self.ws_baseurl + '/ws/' + await self._generate_listenkey())
        return ws

    async def _when2create_new_ws(self):  # todo listenKey过期推送
        # 20小时更新连接一次
        await asyncio.sleep(20 * 3600)

    async def _parse_raw_data(self, raw_data):
        msg = json.loads(raw_data)
        return msg

    async def exit(self):
        super_exit_task = asyncio.create_task(super(BinanceFapiAsyncWs, self).exit())
        if self._session:
            await asyncio.create_task(self._session.close())
        await super_exit_task
