import asyncio, aiohttp
import json
import traceback
from copy import deepcopy

import beeprint
from loguru import logger

import websockets
from websockets import WebSocketClientProtocol
from NoLossAsyncGenerator import no_data_loss_async_generator_decorator


class BinanceWs:
    restful_baseurl = 'https://api.binance.com'
    ws_baseurl = 'wss://stream.binance.com:9443'

    def __init__(self, apikey, secret):
        self._apikey = apikey
        self._secret = secret
        self._session: aiohttp.ClientSession = None
        self._ws: websockets.WebSocketClientProtocol = None
        self._detect_hook = {}  # {future:[{condition1:...,condition2:...},{condition3:...},...]}条件列表中的任何一个条件字典全部达成，便设置结果
        self._ws_ok: asyncio.Future = None

    async def exit(self):
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

    async def _extend_listenkey(self, new_listen_key: str):
        async with self.session.put(
                self.restful_baseurl + '/api/v3/userDataStream',
                headers={'X-MBX-APIKEY': self._apikey},
                data={'listenKey': new_listen_key}
        )as r:
            return await r.json()

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
        async for msg in self._ws:
            msg = json.loads(msg)
            logger.debug('\n' + beeprint.pp(msg, output=False, string_break_enable=False, sort_keys=False))
            self._msg_handler(msg)

    async def _ws_manager(self):
        # 半小时发一个ping
        asyncio.create_task(self._infinity_post_listenKey())
        while True:
            # 20小时一换ws连接
            time_limitted_ws_task = asyncio.create_task(asyncio.wait_for(self._time_limitted_ws(), 20 * 3600))
            try:
                await time_limitted_ws_task
            except asyncio.exceptions.TimeoutError:
                logger.debug('\n' + traceback.format_exc())
            except:
                logger.error('\n' + traceback.format_exc())
            finally:
                if isinstance(self._ws, WebSocketClientProtocol):
                    asyncio.create_task(self._ws.close())

    def _msg_handler(self, news):
        reciever_done = []
        for reciever, _filters in self._detect_hook.items():
            if any([all([value == news[key] for key, value in _filter.items()]) for _filter in _filters]):
                reciever.set_result(deepcopy(news))
                reciever_done.append(reciever)
        for reciever in reciever_done:
            self._detect_hook.pop(reciever)

    @classmethod
    async def create_instance(cls, apikey, secret):
        self = cls(apikey, secret)
        self._ws_ok = asyncio.get_running_loop().create_future()
        # 启动ws管理器
        asyncio.create_task(self._ws_manager())
        await self._ws_ok
        return self

    def _put_hook(self, type):
        '''
        在ws数据流中放置探测钩子

        :param type: 类型，有order、balance等币安user data
        :return:asyncio.Future类型的钩子
        '''
        if type == 'order':
            hook_future = asyncio.get_running_loop().create_future()
            self._detect_hook[hook_future] = [{"e": "executionReport"}]
            return hook_future

    @no_data_loss_async_generator_decorator
    async def watch_order(self):
        hook_future = self._put_hook('order')
        while True:
            msg = await hook_future
            hook_future = self._put_hook('order')
            yield msg


if __name__ == '__main__':
    pass
