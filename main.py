import signal
import argparse
import websocket
import asyncio
import json
import logging
import os
from time import time
from datetime import datetime
from datetime import timedelta


def ts():
    return int(time())


def next_day(dt):
    return dt + timedelta(days=1)


class RequestId:
    def __init__(self):
        self._value = 1

    def get(self):
        self._value += 1
        return self._value - 1


class BinanceMDS:
    def __init__(self, ws_endpoint, symbol, folder='', loop=asyncio.new_event_loop()):
        websocket.enableTrace(False)
        self._ws = websocket.WebSocket()
        self._ws.connect(ws_endpoint)
        self._ws_request_id = RequestId()

        self._loop = loop
        self._is_running = False

        self._symbol = symbol
        self._streams = {'trade': 'trade', 'depth20': 'depth20@100ms'}
        self._headers = {'trade': 'ts, trade id, price, amount, maker side\n',
                         'depth20': 'ts, '
                                    'bid[0].price, bid[0].amount, '
                                    'bid[1].price, bid[1].amount, '
                                    'bid[2].price, bid[2].amount, '
                                    'bid[3].price, bid[3].amount, '
                                    'bid[4].price, bid[4].amount, '
                                    'bid[5].price, bid[5].amount, '
                                    'bid[6].price, bid[6].amount, '
                                    'bid[7].price, bid[7].amount, '
                                    'bid[8].price, bid[8].amount, '
                                    'bid[9].price, bid[9].amount, '
                                    'bid[10].price, bid[10].amount, '
                                    'bid[11].price, bid[11].amount, '
                                    'bid[12].price, bid[12].amount, '
                                    'bid[13].price, bid[13].amount, '
                                    'bid[14].price, bid[14].amount, '
                                    'bid[15].price, bid[15].amount, '
                                    'bid[16].price, bid[16].amount, '
                                    'bid[17].price, bid[17].amount, '
                                    'bid[18].price, bid[18].amount, '
                                    'bid[19].price, bid[19].amount, '

                                    'ask[0].price, ask[0].amount, '
                                    'ask[1].price, ask[1].amount, '
                                    'ask[2].price, ask[2].amount, '
                                    'ask[3].price, ask[3].amount, '
                                    'ask[4].price, ask[4].amount, '
                                    'ask[5].price, ask[5].amount, '
                                    'ask[6].price, ask[6].amount, '
                                    'ask[7].price, ask[7].amount, '
                                    'ask[8].price, ask[8].amount, '
                                    'ask[9].price, ask[9].amount, '
                                    'ask[10].price, ask[10].amount, '
                                    'ask[11].price, ask[11].amount, '
                                    'ask[12].price, ask[12].amount, '
                                    'ask[13].price, ask[13].amount, '
                                    'ask[14].price, ask[14].amount, '
                                    'ask[15].price, ask[15].amount, '
                                    'ask[16].price, ask[16].amount, '
                                    'ask[17].price, ask[17].amount, '
                                    'ask[18].price, ask[18].amount, '
                                    'ask[19].price, ask[19].amount\n'
                         }

        if folder[-1] != '/': folder += '/'
        self._folder = folder + self._symbol + '/'
        self._files = {j: self._folder + j for j in self._streams}

        self._opened_files = None

        self._logger = logging.getLogger('binance_mds:' + self._symbol)

    def start(self):
        self._logger.debug('start')

        self._loop.add_signal_handler(signal.SIGINT, self.stop)
        self._loop.add_signal_handler(signal.SIGTERM, self.stop)

        self._loop.run_until_complete(self.run())

    def stop(self):
        self._logger.debug('stop')

        self._is_running = False

    async def run(self):
        self._logger.debug('run')
        self._logger.info(f'symbol: {self._symbol}')
        self._logger.info(f'streams: {self._streams}')
        self._logger.info(f'files: {self._files}')

        if not os.path.exists(self._folder):
            os.mkdir(self._folder)

        self._opened_files = \
            {
                j:
                    (self._open_or_create(self._files[j] + '_' + str(datetime.today().date()) + '.csv',
                                          self._headers[j]),
                     datetime.today().date())
                for j in self._files
            }

        self._is_running = True

        handle = self._loop.create_task(self._handle())
        await self._subscribe()

        await handle

    def _open_or_create(self, file, header):
        self._logger.info(f'open: {file}')

        was_exist = os.path.exists(file)
        opened_file = open(file, 'a')

        if not was_exist:
            opened_file.write(header)

        return opened_file

    async def _subscribe(self):
        self._logger.info(f'subscribe: {[i for i in self._streams]}')

        await self._write(json.dumps(
            {
                'method': 'SUBSCRIBE',
                'params': [self._symbol + '@' + self._streams[i] for i in self._streams],
                'id': self._ws_request_id.get()
            }
        ))

    async def _handle(self):
        while self._is_running:
            data = json.loads(await self._read())

            if 'lastUpdateId' in data:
                self._opened_files['depth20'][0].write(f'{ts()}')
                for bid in data['bids']:
                    self._opened_files['depth20'][0].write(f', {bid[0]}, {bid[1]}')
                for ask in data['asks']:
                    self._opened_files['depth20'][0].write(f', {ask[0]}, {ask[1]}')
                self._opened_files['depth20'][0].write('\n')

                if next_day(self._opened_files['depth20'][1]) <= datetime.today().date():
                    self._opened_files['depth20'][0].close()
                    self._opened_files['depth20'] = (
                        self._open_or_create(self._files['depth20'] + '_' + str(datetime.today().date()) + '.csv',
                                             self._headers['depth20']),
                        datetime.today().date())

            elif 'e' in data:
                def who_maker(is_buyer_maker):
                    if is_buyer_maker:
                        return 'b'
                    return 's'

                self._opened_files['trade'][0].write(f'{ts()}')
                self._opened_files['trade'][0].write(f', {data["t"]}, {data["p"]}, {data["q"]}, {who_maker(data["m"])}')
                self._opened_files['trade'][0].write('\n')

                if next_day(self._opened_files['trade'][1]) <= datetime.today().date():
                    self._opened_files['trade'][0].close()
                    self._opened_files['trade'] = (
                        self._open_or_create(self._files['trade'] + '_' + str(datetime.today().date()) + '.csv',
                                             self._headers['trade']),
                        datetime.today().date())

    async def _write(self, data):
        self._logger.debug(f'send data: {data}')
        await self._loop.run_in_executor(None, self._ws.send, data)

    async def _read(self):
        data = await self._loop.run_in_executor(None, self._ws.recv)
        self._logger.debug(f'recv data: {data}')
        return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', type=str, dest='symbol')
    parser.add_argument('--folder', type=str, dest='folder')
    parser.add_argument('--logging', type=str, dest='logging', default='info')
    args = parser.parse_args()

    if args.logging == 'info':
        logging.basicConfig(format="[%(levelname)s] - %(name)s: %(message)s", level=logging.INFO)
    elif args.logging == 'debug':
        logging.basicConfig(format="[%(levelname)s] - %(name)s: %(message)s", level=logging.DEBUG)
    else:
        exit(-1)

    binance_mds = BinanceMDS('wss://stream.binance.com:9443/ws',
                             symbol=args.symbol,
                             folder=args.folder)

    binance_mds.start()
