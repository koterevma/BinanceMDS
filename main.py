import argparse
import errno
import json
import logging
import logging.handlers
import signal
import websocket
import functools
import datetime
import headers

from copy import deepcopy as copy
from pathlib import Path
from time import time
from typing import Union


def ts():
    return int(time())


class BinanceMDS:
    def __init__(self, ws_endpoint: str, symbol: str, directory: str = ''):
        websocket.enableTrace(False)
        self._ws = websocket.WebSocket()
        self._ws_endpoint = ws_endpoint
        self._ws_next_id = 1
        self._ws_requests = {}

        self._symbol = symbol
        self._streams = {'trade': 'trade', 'depth20': 'depth20@100ms'}

        self._headers = headers.get_headers()

        self._directory = Path(directory) / self._symbol
        self._files: dict[str, Path] = {j: (self._directory / (self._symbol + "_" + j)) for j in self._streams}

        self._current_files_date = None
        self._opened_files = dict()

        self._is_running = False

        self._logger = logging.getLogger('binance_mds:' + self._symbol)

        signal.signal(signal.SIGINT, functools.partial(self.stop, self).func)
        signal.signal(signal.SIGTERM, functools.partial(self.stop, self).func)

    def run(self):
        self._logger.debug('run')
        self._logger.info(f'symbol: {self._symbol}')
        self._logger.info(f'streams: {self._streams}')
        self._logger.info(f'files: {self._files}')

        if not self._directory.is_dir():
            self._directory.mkdir(parents=True)

        self._is_running = True

        self._init()
        self._handle()
        self._deinit()

    def stop(self, *unused):
        self._logger.debug('stop')

        self._is_running = False

    def _init(self):
        self._logger.debug('init')

        self._open_today_files()

        self._ws.connect(self._ws_endpoint)
        self._subscribe()

    def _deinit(self):
        self._logger.debug('deinit')

        self._close_files()

        if self._ws.getstatus():
            self._ws.close()

    def _reinit(self):
        self._logger.debug('reinit')

        self._deinit()
        self._init()

    def _open_today_files(self):
        self._current_files_date = datetime.date.today()
        self._opened_files = \
            {
                stream:
                    self._open_or_create(
                        self._format_filename_for_date(
                            self._files[stream].with_suffix('.csv'),
                        ),
                        self._headers[stream]
                    )
                for stream in self._files
            }

    def _close_files(self):
        for file in self._opened_files.values():
            file.close()

    def _format_filename_for_date(self, file: Path) -> Path:
        return file.with_name(file.stem + '_' + str(self._current_files_date) + file.suffix)

    def _open_or_create(self, file: Path, header):
        self._logger.info(f'opening: {file}')

        file_exists = file.is_file()
        opened_file = file.open('a')

        if not file_exists:
            opened_file.write(header)

        return opened_file

    def _subscribe(self):
        self._logger.info(f'subscribe: {[i for i in self._streams]}')

        self._send_request(
            {
                'method': 'SUBSCRIBE',
                'params': [self._symbol + '@' + self._streams[i] for i in self._streams]
            }
        )

    def _handle(self):
        while self._is_running:
            data = json.loads(self._read())

            if datetime.date.today() > self._current_files_date:
                self._reinit()

            if 'lastUpdateId' in data:
                self._opened_files['depth20'].write(f'{ts()}')
                for bid in data['bids']:
                    self._opened_files['depth20'].write(f', {bid[0]}, {bid[1]}')
                for ask in data['asks']:
                    self._opened_files['depth20'].write(f', {ask[0]}, {ask[1]}')
                self._opened_files['depth20'].write('\n')

            elif 'e' in data:
                def who_maker(is_buyer_maker):
                    if is_buyer_maker:
                        return 'b'
                    return 's'

                self._opened_files['trade'].write(f'{ts()}')
                self._opened_files['trade'].write(f', {data["t"]}, {data["p"]}, {data["q"]}, {who_maker(data["m"])}')
                self._opened_files['trade'].write('\n')

            elif 'id' in data:
                if 'error' in data:
                    self.stop()

                    self._logger.error(f'{self._ws_requests[data["id"]]}, code: {data["error"]["code"]}')
                else:
                    self._logger.info(f'{self._ws_requests[data["id"]]}, result: {data["result"]}')

    def _send_request(self, data: dict):
        self._ws_requests.update(copy({self._ws_next_id: data}))
        data.update({'id': self._ws_next_id})

        self._write(json.dumps(data))

        self._ws_next_id += 1

    def _write(self, data: str):
        self._logger.debug(f'send data: {data}')

        return self._ws.send(data)

    def _read(self) -> str:
        data = self._ws.recv()

        self._logger.debug(f'recv data: {data}')

        return data


def setup_logging(log_level: str, log_dir: Union[str, Path], symbol: str, num_log_keep: int):
    log_dir = Path(log_dir)
    numeric_level = getattr(logging, log_level.upper(), None)
    if not log_dir.is_dir():
        try:
            log_dir.mkdir()
        except OSError as e:
            if e.errno == errno.EACCES:
                raise RuntimeError(f'Unable to create {log_dir.absolute()}.'
                                   ' Please create directory with appropriate permissions')
            else:
                raise

    log_file = (log_dir / symbol).with_suffix('.log')
    # https://docs.python.org/3/library/logging.handlers.html#timedrotatingfilehandler
    log_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        backupCount=num_log_keep,
        when='midnight'
    )

    logging.basicConfig(
        datefmt='%y-%m-%d %H:%M:%S',
        format='%(asctime)s [%(levelname)s] - %(name)s: %(message)s',
        handlers=(log_handler,),
        level=numeric_level
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', type=str)
    parser.add_argument('-d', '--directory', type=str, default='.')
    parser.add_argument('-l', '--log', type=str, default='info',
                        help='Logging level')
    parser.add_argument('-g', '--log-dir', type=str,
                        default='/var/log/BinanceMDS',
                        help='Directory in which logs are stored')
    parser.add_argument('-k', '--log-keep', type=int, default=2,
                        help='Number of logs to keep after rotation')
    args = parser.parse_args()

    setup_logging(args.log, args.log_dir, args.symbol, args.log_keep)

    binance_mds = BinanceMDS('wss://stream.binance.com:9443/ws',
                             symbol=args.symbol,
                             directory=args.directory)

    binance_mds.run()
