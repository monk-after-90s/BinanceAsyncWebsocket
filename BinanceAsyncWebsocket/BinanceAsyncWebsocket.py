import asyncio, aiohttp
import json
import traceback
from copy import deepcopy
from collections import deque
import beeprint
from loguru import logger

import websockets
from websockets import WebSocketClientProtocol
from NoLossAsyncGenerator import NoLossAsyncGenerator, no_data_loss_async_generator_decorator


class BinanceWs:
    restful_baseurl = 'https://api.binance.com'
    ws_baseurl = 'wss://stream.binance.com:9443'

    def __init__(self, apikey, secret):
        self._apikey = apikey
        self._secret = secret
        self._session: aiohttp.ClientSession = None
        self._ws: websockets.WebSocketClientProtocol = None
        self._ws_generator: NoLossAsyncGenerator = None
        self._ws_ok: asyncio.Future = None
        self._ws_data_containers = deque(maxlen=20,
                                         iterable=[asyncio.get_running_loop().create_future() for i in range(20)])

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
        self._ws_generator = NoLossAsyncGenerator(self._ws)
        async for msg in self._ws_generator:
            msg = json.loads(msg)
            logger.debug('\n' + beeprint.pp(msg, output=False, string_break_enable=False, sort_keys=False))
            self._msg_handler(msg)

    async def _ws_manager(self):
        # 半小时发一个ping
        asyncio.create_task(self._infinity_post_listenKey())
        while True:
            # 20小时一换ws连接
            time_limitted_ws_task = asyncio.create_task(self._time_limitted_ws())

            async def sleep_then_raise():
                await asyncio.sleep(20 * 3600)
                raise TimeoutError('Time to change ws.')

            try:
                await asyncio.gather(time_limitted_ws_task, sleep_then_raise())
            except TimeoutError as e:  # 正常更换
                if str(e) == 'Time to change ws.' and isinstance(self._ws_generator, NoLossAsyncGenerator):
                    # 等待可能累积的数据全部吐出来并关闭
                    await self._ws_generator.close()
                logger.debug('\n' + traceback.format_exc())
            except:  # 异常更换
                logger.error('\n' + traceback.format_exc())
            finally:
                if isinstance(self._ws, WebSocketClientProtocol):
                    asyncio.create_task(self._ws.close())

    def _find_first_pending_future_index(self):
        # 找第一个pending的future

        target_index = 0
        for i in range(len(self._ws_data_containers) - 1, -1, -1):
            if self._ws_data_containers[i].done():
                target_index = i + 1
                break
        return target_index

    def _msg_handler(self, news):
        target_index = self._find_first_pending_future_index()
        # 要设置值的future
        self._ws_data_containers[target_index].set_result(news)
        # 补上至少一半队列数个pending future
        if target_index > self._ws_data_containers.maxlen // 2:
            for _ in range(target_index - self._ws_data_containers.maxlen // 2):
                self._ws_data_containers.append(asyncio.get_running_loop().create_future())

    def _fill_futures(self):
        '''
        填充future直到至少一半队列数个pending future

        :return:
        '''
        first_pending_future_index = self._find_first_pending_future_index()

        # 补上至少一半队列数个pending future
        if first_pending_future_index > self._ws_data_containers.maxlen // 2:
            for _ in range(first_pending_future_index - self._ws_data_containers.maxlen // 2):
                self._ws_data_containers.append(asyncio.get_running_loop().create_future())

    @classmethod
    async def create_instance(cls, apikey, secret):
        self = cls(apikey, secret)
        self._ws_ok = asyncio.get_running_loop().create_future()
        # 启动ws管理器
        asyncio.create_task(self._ws_manager())
        await self._ws_ok
        return self

    # def _put_hook(self, type):
    #     '''
    #     在ws数据流中放置探测钩子
    #
    #     :param type: 类型，有order、balance等币安user data
    #     :return:asyncio.Future类型的钩子
    #     '''
    #     if type == 'order':
    #         hook_future = asyncio.get_running_loop().create_future()
    #         self._detect_hook[hook_future] = [{"e": "executionReport"}]
    #         return hook_future

    @no_data_loss_async_generator_decorator
    async def watch_order(self):
        last_future = None
        while True:
            if not last_future:
                current_future = self._ws_data_containers[self._find_first_pending_future_index()]
            else:
                while True:
                    try:
                        current_future = self._ws_data_containers[self._ws_data_containers.index(last_future) + 1]
                    except IndexError:
                        self._safely_replace_longer_q()
                        current_future = self._ws_data_containers[self._find_first_pending_future_index()]
                        break
            yield deepcopy(await current_future)
            last_future = current_future
        # hook_future = self._put_hook('order')
        # while True:
        #     msg = await hook_future
        #     hook_future = self._put_hook('order')
        #     yield msg

    def _safely_replace_longer_q(self):
        '''
        安全的更换更长的futures deque，即_ws_data_containers

        :return:
        '''
        self._ws_data_containers = deque(iterable=self._ws_data_containers,
                                         maxlen=self._ws_data_containers.maxlen + 5)  # 扩展5个位置
        # 补齐future
        self._fill_futures()
        logger.debug(
            f'Extend deque:\n{beeprint.pp(self._ws_data_containers, sort_keys=False, string_break_enable=False)}')

if __name__ == '__main__':
    pass
