import email
import json
import os
import re
import logging
from contextlib import contextmanager, closing
from datetime import datetime
from functools import partial
from logging import handlers
from pathlib import Path

from lxml import html
import prequests as requests

from generator import generator
from processor import Processor

log = logging.getLogger(__name__)
now = datetime.now()


def get_perf_outlook(text):
    if '#ff4d52' in text:
        return 'sell'
    if '#464e56' in text:
        return 'hold'
    if '#1ac567' in text:
        return 'buy'


def as_tuple(list_tuple_item_or_none):
    if list_tuple_item_or_none is None:
        return ()
    if not isinstance(list_tuple_item_or_none, (list, tuple)):
        return (list_tuple_item_or_none,)

    return list_tuple_item_or_none


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


def safe(func, ignore_exceptions=None):
    with context(verbose=False, ignore_exceptions=ignore_exceptions):
        return func()


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

        ignore = (IndexError, TypeError)
        data['eval'] = safe(lambda: tree.xpath('//*[@id="quote-summary"]/div[3]/div[1]/div[2]/div[2]')[0].text, ignore)

        est_return = tree.xpath('//*[@id="quote-summary"]/div[3]/div[1]/div[3]/div[1]')
        data['est_return'] = safe(lambda: int(re.match(r'-?\d+', est_return[0].text)[0]), ignore)  # '-18% Est. Return' -> -18

        data['pattern'] = safe(lambda: tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[2]/div[1]/span[1]/span')[0].text, ignore)
        data['pattern_type'] = safe(lambda: tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[2]/div[2]/p')[0].text, ignore)
        data['recmd_short'] = safe(lambda: get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[1]/a/div[1]/div[2]/svg/@style')[0]), ignore)
        data['recmd_mid'] = safe(lambda: get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[2]/a/div[1]/div[2]/svg/@style')[0]), ignore)
        data['recmd_long'] = safe(lambda: get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[3]/a/div[1]/div[2]/svg/@style')[0]), ignore)
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

    processor = Processor(20)
    not_found = 'ALFA0421 ALFAperp0222 ALFA0420EU ALFA0430 AKZM BEL0223 BMW BAST CBOM0224EU BEL0627 ALFAperp CHMF1022 DME0223 EGPT0329 EVRZ0323 EGPT0431 GAZP0327 GPBperp GAZP0322 GAZP0837 GAZP1124 GLPR0923 GMKN1022 KCEL ISBNK0424 KZTK LUK1120 LUK1126 LUK0423 MTS0620 OMA0128 PLZL0322 OMA0148 NVTK0221 RSHB1023 RZD1020 RZD0527 PLZL0223 SBER0222 SCFL0623 SGENperp SPB ROSN0322 TRY0141 TRY0130 TRY0228 STLC0821 TRY1028 TRY0336 TRY0234 TRY0225 TUN1023 VAKI0324 TUN0726 VTB1020 XS0935311240 TTEL0225 XS1693971043 XS1319813769 XS0893212398 XS0559915961 VTBperp XS0885736925 XS0191754729 XS1603335610 XS0848530977 XS1752568144 XS1577953174 XS0304274599'
    not_found = not_found.split()
    log.info('Started')
    with closing(result_writer(file_name=jsons_path / f'data_{now_str}.json')) as on_result:
        on_result.send(None)
        tickers = get_tickers()
        for ticker in tickers:
            if ticker not in not_found:
                processor.add(partial(crawl, ticker=ticker, on_result=on_result))
        try:
            processor.wait_done()
        except KeyboardInterrupt:
            log.info('Ctrl+C pressed')
        finally:
            processor.stop()
    log.info('Finished')


if __name__ == '__main__':
    main()