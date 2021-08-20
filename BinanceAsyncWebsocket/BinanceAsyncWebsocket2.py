import asyncio, aiohttp
import json
import websockets
from AsyncWebsocketStreamInterface import AsyncWebsocketStreamInterface


class BinanceWs(AsyncWebsocketStreamInterface):
    restful_baseurl = 'https://api.binance.com'
    ws_baseurl = 'wss://stream.binance.com:9443'

    @classmethod
    async def create_instance(cls, apikey):
        self = cls(apikey)
        return self

    def __init__(self, apiKey):
        self.apiKey = apiKey
        self.session: aiohttp.ClientSession = aiohttp.ClientSession()
        super(BinanceWs, self).__init__()
        asyncio.create_task(self._infinity_post_listenKey())

    async def _generate_listenkey(self):
        async with self.session.post(
                self.restful_baseurl + '/api/v3/userDataStream',
                headers={'X-MBX-APIKEY': self.apiKey},
        ) as r:
            listenKey = (await r.json())['listenKey']
            print(listenKey)
            return listenKey

    async def _infinity_post_listenKey(self):
        while True:
            await asyncio.create_task(asyncio.sleep(30 * 60))
            await self._generate_listenkey()

    async def exit(self):
        super_exit_task = asyncio.create_task(super(BinanceWs, self).exit())
        await self.session.close()
        await super_exit_task

    async def _create_ws(self):
        return await websockets.connect(self.ws_baseurl + '/ws/' + await self._generate_listenkey())

    async def _when2create_new_ws(self):
        await asyncio.sleep(20 * 3600)

    async def _parse_raw_data(self, raw_data):
        return json.loads(raw_data)

    def filter_stream(self, _filters: list = None):
        '''
        Filter the ws data stream and push the filtered data to the async generator which is returned by the method.
        Remember to explicitly call the close method of the async generator to close the stream.

        stream=binancews.filter_stream()

        #handle message in one coroutine:
        async for news in stream:
            ...
        #close the stream in another:
        close_task=asyncio.create_task(stream.close())
        ...
        await close_task


        :param _filters:
        :return:
        '''
        return self.stream_filter(_filters)

    def order_stream(self):
        '''
        Filter the ws order data stream and push the filtered data to the async generator which is returned by the method.
        Remember to explicitly call the close method of the async generator to close the stream.


        stream=binancews.order_stream()

        #handle message in one coroutine:
        async for news in stream:
            ...
        #close the stream in another:
        close_task=asyncio.create_task(stream.close())
        ...
        await close_task

        :return:
        '''
        return self.filter_stream([{"e": "executionReport"}])


if __name__ == '__main__':
    pass
