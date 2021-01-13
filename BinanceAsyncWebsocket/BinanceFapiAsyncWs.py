import aiohttp
from AsyncWebsocketStreamInterface import AsyncWebsocketStreamInterface
import asyncio


class BinanceFapiAsyncWs(AsyncWebsocketStreamInterface):
    ws_baseurl = 'wss://fstream.binance.com'
    restful_baseurl = 'https://fapi.binance.com'

    def __init__(self, apikey):
        super(BinanceFapiAsyncWs, self).__init__()
        self._apikey = apikey
        self._session: aiohttp.ClientSession = None

    @property
    def session(self):
        if not self._session:
            self._session = aiohttp.ClientSession()

        return self._session

    async def _generate_listenkey(self, debug=False):
        # ts = int(datetime.datetime.utcnow().timestamp() * 1000)
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

    async def _create_ws(self):
        pass

    async def _parse_raw_data(self, raw_data):
        pass

    async def _when2create_new_ws(self):
        pass

    async def exit(self):
        super_exit_task = asyncio.create_task(super(BinanceFapiAsyncWs, self).exit())
        if self._session:
            await asyncio.create_task(self._session.close())
        await super_exit_task
