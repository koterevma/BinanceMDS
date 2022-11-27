import argparse
import asyncio
import errno
import json
import logging
import logging.handlers
import signal
import websocket

from datetime import datetime
from datetime import timedelta
from copy import deepcopy as copy
from pathlib import Path
from time import time
from typing import Optional


def ts():
    return int(time())


def next_day(dt):
    return dt + timedelta(days=1)


class BinanceMDS:
    def __init__(self, ws_endpoint: str, symbol: str, logs_dir: str, directory: str = '',
                 loop: Optional[asyncio.AbstractEventLoop] = None):
        websocket.enableTrace(False)
        self._ws = websocket.WebSocket()
        self._ws.connect(ws_endpoint)
        self._ws_next_id = 1
        self._ws_requests = {}

        if loop is None:
            loop = asyncio.new_event_loop()
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


        self._directory = Path(directory)
        self._files: dict[str, Path] = {j: (self._directory / j) for j in self._streams}

        self._opened_files = dict()

        self._logs_dir = Path(logs_dir)
        self._setup_logger()

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

        if not self._directory.is_dir():
            self._directory.mkdir()

        self._opened_files = \
            {
                stream:
                    (
                        self._open_or_create(
                            self._format_filename_for_current_date(
                                self._files[stream].with_suffix('.csv')
                            ),
                            self._headers[stream]
                        ),
                        datetime.today().date()
                    )
                    for stream in self._files
            }

        self._is_running = True

        handle = self._loop.create_task(self._handle())
        await self._subscribe()

        await handle

    def _setup_logger(self):
        self._logger = logging.getLogger('binance_mds:' + self._symbol)

        if not self._logs_dir.is_dir():
            try:
                self._logs_dir.mkdir()
            except OSError as e:
                if e.errno == errno.EACCES:
                    raise RuntimeError(f'Unable to create {self._logs_dir.absolute()}.'
                                       ' Please create directory with appropriate permissions')
                else:
                    raise

        log_file = Path(self._logs_dir, self._symbol).with_suffix('.log')
        # https://docs.python.org/3/library/logging.handlers.html#watchedfilehandler
        log_handler = logging.handlers.WatchedFileHandler(log_file)

        self._logger.addHandler(log_handler)

    @staticmethod
    def _format_filename_for_current_date(file: Path) -> Path:
        return file.with_name(file.stem + '_' + str(datetime.today().date()) + file.suffix)

    def _open_or_create(self, file: Path, header):
        self._logger.info(f'opening: {file}')

        file_exists = file.is_file()
        opened_file = file.open('a')

        if not file_exists:
            opened_file.write(header)

        return opened_file

    async def _subscribe(self):
        self._logger.info(f'subscribe: {[i for i in self._streams]}')

        await self._write(json.dumps(self._add_id(
            {
                'method': 'SUBSCRIBE',
                'params': [self._symbol + '@' + self._streams[i] for i in self._streams]
            }
        )))

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
                        self._open_or_create(
                            self._format_filename_for_current_date(
                                self._files['depth20'].with_suffix('.csv')
                            ),
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
                        self._open_or_create(
                            self._format_filename_for_current_date(
                                self._files['trade'].with_suffix('.csv')
                            ),
                            self._headers['trade']),
                        datetime.today().date())

            elif 'id' in data:
                if 'error' in data:
                    self._logger.error(f'{self._ws_requests[data["id"]]}, code: {data["error"]["code"]}')

                    self.stop()
                else:
                    self._logger.info(f'{self._ws_requests[data["id"]]}, result: {data["result"]}')

    def _add_id(self, data: dict) -> dict:
        self._ws_requests.update(copy({self._ws_next_id: data}))
        data.update({'id': self._ws_next_id})

        self._ws_next_id += 1

        return data

    async def _write(self, data: str):
        self._logger.debug(f'send data: {data}')
        await self._loop.run_in_executor(None, self._ws.send, data)

    async def _read(self) -> str:
        data = await self._loop.run_in_executor(None, self._ws.recv)
        self._logger.debug(f'recv data: {data}')
        return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', type=str)
    parser.add_argument('-d', '--directory', type=str, default='.')
    parser.add_argument('-l', '--log-level', type=str, default='info',
                        help='Logging level')
    parser.add_argument('-g', '--logs-dir', type=str,
                        default='/var/log/BinanceMDS',
                        help='Directory in which logs are stored')
    args = parser.parse_args()

    numeric_level = getattr(logging, args.log_level.upper(), None)
    logging.basicConfig(
        format="[%(levelname)s] - %(name)s: %(message)s",
        level=numeric_level
    )

    binance_mds = BinanceMDS('wss://stream.binance.com:9443/ws',
                             symbol=args.symbol,
                             directory=args.directory,
                             logs_dir=args.logs_dir)

    binance_mds.start()
