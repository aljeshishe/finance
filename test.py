import logging

import requests

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s|%(levelname)-4.4s|%(thread)s|%(filename)-10.10s|%(funcName)-10.10s|%(message)s',
                    handlers=[logging.StreamHandler(),
                              ])
print('Testing how fast finance.yahoo.com blocks scraping')

while True:
    try:
        requests = requests.Session()
        r = requests.get('https://finance.yahoo.com/quote/ZYNE',
                         proxies=dict(https='94.135.230.170:443'),
                         headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0'}
                         )

        logging.info(f'{r.status_code}  {len(r.text)} {"Overvalued" in r.text} {"Commodity Channel Index"  in r.text}  ')
    except Exception as e:
        print(e)