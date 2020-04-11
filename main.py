import email
import json
import os
import re
import logging
from contextlib import contextmanager, closing
from datetime import datetime
from logging import handlers
from pathlib import Path

from lxml import html
import prequests as requests

from generator import generator
from processor import Processor
from src.prequests.prequests.utils import as_tuple

log = logging.getLogger(__name__)
now = datetime.now()

def get_perf_outlook(text):
    if '#ff4d52' in text:
        return 'sell'
    if '#464e56' in text:
        return 'hold'
    if '#1ac567' in text:
        return 'buy'

@contextmanager
def context(verbose=True, ignore_exceptions=None, raise_exceptions=None, **kwargs):
    ignore_exceptions = as_tuple(ignore_exceptions)
    raise_exceptions = as_tuple(raise_exceptions)

    to_str = lambda d:  ' '.join(map(lambda i: f'{i[0]}={i[1]}', d.items()))
    kwargs_str = to_str(kwargs)
    if verbose:
        log.info(f'Processing {kwargs_str}')
    try:
        yield kwargs
        if verbose:
            log.info(f'Finished processing {to_str(kwargs)}')
    except Exception as e:
        no_traceback = any([isinstance(e, exc) for exc in ignore_exceptions])
        exc_type = e.__class__.__name__ if no_traceback else ''
        log.exception(f'Exception {exc_type} while processing {to_str(kwargs)}', exc_info=not no_traceback)
        if any([isinstance(e, exc) for exc in raise_exceptions]):
            raise e


def crawl(on_result, ticker):
    url = f'https://finance.yahoo.com/quote/{ticker}'
    with context(url=url, verbose=False):
        response = requests.get(url)
        response.raise_for_status()
        if 'lookup' in response.url:
            log.warning(f'Ticker {ticker} not found ({url})')
            return

        tree = html.fromstring(response.text)
        data = {}
        data['ticker'] = ticker
        evaluation = tree.xpath('//*[@id="quote-summary"]/div[3]/div[1]/div[2]/div[2]')[0].text
        data['eval'] = evaluation
        est_return = tree.xpath('//*[@id="quote-summary"]/div[3]/div[1]/div[3]/div[1]')[0].text
        if est_return:
            data['est_return'] = int(re.match(r'-?\d+', est_return)[0])  # '-18% Est. Return' -> -18

        pattern = tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[2]/div[1]/span[1]/span')[0].text
        data['pattern'] = pattern
        data['pattern_type'] = tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[2]/div[2]/p')[0].text

        data['recmd_short'] = get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[1]/a/div[1]/div[2]/svg/@style')[0])
        data['recmd_mid'] = get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[2]/a/div[1]/div[2]/svg/@style')[0])
        data['recmd_long'] = get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[3]/a/div[1]/div[2]/svg/@style')[0])
        on_result.send(data)


def get_tickers():
    import csv
    with open('ListingSecurityList.csv', encoding='cp1251') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        result = [row[1] for row in reader]
        return result


def result_writer(file_name):
    temp_file_name = file_name.with_suffix('.tmp')
    log.info(f'Started writing results to {temp_file_name}')
    try:
        with open(temp_file_name, 'w', encoding='utf-8') as fp:
            while True:
                data = yield
                log.debug(' '.join(map(lambda i: f'{i[0]}:{i[1]}', data.items())))
                data['crawl_datetm'] = str(now)
                fp.write('{}\n'.format(json.dumps(data, ensure_ascii=False)))
                fp.flush()
    except GeneratorExit:
        log.info(f'Finished writing results. Renaming {temp_file_name} to {file_name}')
        os.rename(temp_file_name, file_name)




def main():
    requests.Proxies.instance(throttling_interval_secs=2, temp_dir='stats')
    now_str = datetime.now().strftime('%y%m%d_%H%M%S')
    jsons_path = Path('jsons')
    jsons_path.mkdir(parents=True, exist_ok=True)
    Path('logs').mkdir(parents=True, exist_ok=True)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s|%(levelname)-4.4s|%(thread)s|%(filename)-10.10s|%(funcName)-10.10s|%(message)s',
                        handlers=[logging.StreamHandler(),
                                  handlers.RotatingFileHandler(f'logs/finance_{now_str}.log',
                                                                       maxBytes=200 * 1024 * 1024, backupCount=5)
                                  ])

    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    processor = Processor(50)

    log.info('Started')
    with closing(result_writer(file_name=jsons_path / f'data_{now_str}.json')) as on_result:
        on_result.send(None)
        tickers = get_tickers()
        processor.add_tasks(generator(crawl, ticker=tickers, on_result=on_result))
        try:
            processor.wait_done()
        except KeyboardInterrupt:
            log.info('Ctrl+C pressed')
        finally:
            processor.stop()
    log.info('Finished')


if __name__ == '__main__':
    main()