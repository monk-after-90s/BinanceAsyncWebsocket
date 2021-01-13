import asyncio, aiohttp
import json
import traceback
from copy import deepcopy

import beeprint
from ensureTaskCanceled import ensureTaskCanceled
from loguru import logger

import websockets
from websockets import WebSocketClientProtocol
from NoLossAsyncGenerator import NoLossAsyncGenerator


class BinanceWs:
    restful_baseurl = 'https://api.binance.com'
    ws_baseurl = 'wss://stream.binance.com:9443'

    def __init__(self, apikey):
        self._apikey = apikey
        self._session: aiohttp.ClientSession = None
        self._ws: websockets.WebSocketClientProtocol = None
        self._ws_generator: NoLossAsyncGenerator = None
        self._ws_ok: asyncio.Future = None
        self._handlers = set()
        self._exiting = False

    async def exit(self):
        self._exiting = True
        session_close_task = None
        ws_close_task = None
        if self._session:
            session_close_task = asyncio.create_task(self._session.close())
        if self._ws:
            ws_close_task = asyncio.create_task(self._ws.close())
        if session_close_task:
            await session_close_task
        if ws_close_task:
            await ws_close_task

    # def _generate_signature(self, **kwargs):
    #     if 'recvWindow' not in kwargs.keys():
    #         kwargs['recvWindow'] = 5000
    #     if 'timestamp' not in kwargs.keys():
    #         kwargs['timestamp'] = int(datetime.datetime.utcnow().timestamp() * 1000)
    #     params = [(str(key) + '=' + str(value)) for key, value in kwargs.items()]
    #     msg = '&'.join(params).encode('utf-8')
    #     return hmac.new(self._secret.encode('utf-8'), msg, digestmod=hashlib.sha256).hexdigest()

    @property
    def session(self):
        if not self._session:
            self._session = aiohttp.ClientSession()

        return self._session

    async def _generate_listenkey(self):
        # ts = int(datetime.datetime.utcnow().timestamp() * 1000)
        async with self.session.post(
                self.restful_baseurl + '/api/v3/userDataStream',
                headers={'X-MBX-APIKEY': self._apikey},
                # data={
                #     'recvWindow': 5000,
                #     'timestamp': ts,
                #     'signature': self._generate_signature(recvWindow=5000, timestamp=ts)}
        ) as r:
            listenKey = (await r.json())['listenKey']
            print(listenKey)
            return listenKey

    # async def _extend_listenkey(self, new_listen_key: str):
    #     async with self.session.put(
    #             self.restful_baseurl + '/api/v3/userDataStream',
    #             headers={'X-MBX-APIKEY': self._apikey},
    #             data={'listenKey': new_listen_key}
    #     )as r:
    #         return await r.json()

    async def _close_listenkey(self, new_listen_key: str):
        async with self.session.delete(
                self.restful_baseurl + '/api/v3/userDataStream',
                headers={'X-MBX-APIKEY': self._apikey},
                data={'listenKey': new_listen_key}
        )as r:
            return await r.json()

    async def _infinity_post_listenKey(self):
        while True:
            await asyncio.create_task(asyncio.sleep(30 * 60))
            await self._generate_listenkey()

    async def _time_limitted_ws(self):
        self._ws = await websockets.connect(self.ws_baseurl + '/ws/' + await self._generate_listenkey())
        # 通知实例化完成
        if not self._ws_ok.done():
            self._ws_ok.set_result(None)
        # 传递值
        self._ws_generator = NoLossAsyncGenerator(self._ws)
        async for msg in self._ws_generator:
            msg = json.loads(msg)
            logger.debug('\n' + beeprint.pp(msg, output=False, string_break_enable=False, sort_keys=False))
            tasks = []
            for handler in self._handlers:
                if asyncio.iscoroutinefunction(handler):
                    tasks.append(asyncio.create_task(handler(deepcopy(msg))))
                else:
                    handler(deepcopy(msg))
            [await task for task in tasks]

    async def _ws_manager(self):
        # 半小时发一个ping
        asyncio.create_task(self._infinity_post_listenKey())
        while not self._exiting:
            # 20小时一换ws连接
            time_limitted_ws_task = asyncio.create_task(self._time_limitted_ws())

            async def sleep_then_raise():
                await asyncio.sleep(20 * 3600)
                raise TimeoutError('Time to change ws.')

            try:
                await asyncio.gather(time_limitted_ws_task, sleep_then_raise())
            except TimeoutError as e:  # 正常更换
                if str(e) == 'Time to change ws.' and isinstance(self._ws_generator, NoLossAsyncGenerator):
                    logger.debug('\n' + traceback.format_exc())
                else:  # 异常更换
                    logger.error('\n' + traceback.format_exc())
            except:  # 异常更换
                logger.error('\n' + traceback.format_exc())
            finally:
                if isinstance(self._ws, WebSocketClientProtocol):
                    # 等待可能累积的数据全部吐出来并关闭
                    await self._ws_generator.close()
                    asyncio.create_task(self._ws.close())
                    asyncio.create_task(ensureTaskCanceled(time_limitted_ws_task))

    @classmethod
    async def create_instance(cls, apikey):
        self = cls(apikey)
        self._ws_ok = asyncio.get_running_loop().create_future()
        # 启动ws管理器
        asyncio.create_task(self._ws_manager())
        await self._ws_ok
        return self

    def add_order_handler(self, handler):
        '''
        添加订单相关数据的处理函数或者异步函数，形如
        def handler(msg):
            ...
        或
        async def handler(msg):
            ...

        :param new_handler:
        :return:
        '''
        # 异步函数
        if asyncio.iscoroutinefunction(handler):
            async def new_handler(msg):
                if msg['e'] == "executionReport":
                    return await handler(msg)

            self._handlers.add(new_handler)
        else:
            def new_handler(msg):
                if msg['e'] == "executionReport":
                    return handler(msg)

            self._handlers.add(new_handler)

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
        if _filters is None:
            _filters = []

        ag = NoLossAsyncGenerator(None)

        def handler(msg):
            if (_filters and any(
                    [all([((key in msg) and (value == msg[key])) for key, value in _filter.items()]) for _filter in
                     _filters])) \
                    or not _filters:
                ag.q.put_nowait(msg)

        self._handlers.add(handler)
        _close = ag.close

        async def close():
            self._handlers.remove(handler)
            await _close()

        ag.close = close
        return ag

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
