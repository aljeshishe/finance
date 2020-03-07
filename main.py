import email
import re
import logging
from datetime import datetime

import requests
from lxml import html

import model
from model import Item


log = logging.getLogger(__name__)
def get_perf_outlook(text):
    if '#ff4d52' in text:
        return model.Recomendation.SELL
    if '#464e56' in text:
        return model.Recomendation.HOLD
    if '#1ac567' in text:
        return model.Recomendation.BUY


def crawl(ticker):
    headers = '''User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36
Accept-Encoding: gzip, deflate, br
'''

    headers = email.message_from_string(headers)

    response = requests.get(f'https://finance.yahoo.com/quote/{ticker}', headers=headers)
    response.raise_for_status()

    tree = html.fromstring(response.text)
    item = Item()
    item.ticker = ticker
    item.insert_date_time = datetime.now()
    evaluation = tree.xpath('//*[@id="quote-summary"]/div[3]/div[1]/div[2]/div[2]')[0].text
    if evaluation == 'Overvalued':
        item.eval = model.Evaluation.OVERVALUED
    elif evaluation == 'Near Fair Value':
        item.eval = model.Evaluation.NEAR_FAIR_VALUE
    elif evaluation == 'Overvalued':
        item.eval = model.Evaluation.OVERVALUED

    est_return = tree.xpath('//*[@id="quote-summary"]/div[3]/div[1]/div[3]/div[1]')[0].text
    if est_return:
        item.est_return = int(re.match(r'-?\d+', est_return)[0])  # '-18% Est. Return' -> -18

    pattern = tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[2]/div[1]/span[1]/span')[0].text
    if pattern == 'Bearish':
        item.pattern = model.Pattern.Bearish
    elif pattern == 'Bullish':
        item.pattern = model.Pattern.Bullish
    elif pattern == 'Neutral':
        item.pattern = model.Pattern.Neutral

    item.pattern_type = tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[2]/div[2]/p')[0].text

    item.recmd_short = get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[1]/a/div[1]/div[2]/svg/@style')[0])
    item.recmd_mid = get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[2]/a/div[1]/div[2]/svg/@style')[0])
    item.recmd_long = get_perf_outlook(tree.xpath('//*[@id="interactive-2col-qsp-m"]/div[3]/div[3]/ul/li[3]/a/div[1]/div[2]/svg/@style')[0])
    print(f'{item=}')

    session = model.Session()
    session.add(item)
    session.commit()


def get_tickers():
    import csv
    with open('ListingSecurityList.csv', encoding='cp1251') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        result = [row[1] for row in reader]
        return result


def main():
    tickers = 'MSFT CRM SYK WM CTAS EL SYY AAPL ALL RNG SPGI NDAQ CME MA INTU V NVDA GOOG ADBE AMZN PGR FB INTC IDXX UNH TSLA TXN LRCX MAS'.split()
    tickers = get_tickers()
    for ticker in tickers:
        try:
            crawl(ticker)
        except Exception as e:
            log.exception(f'Got exception while crawling {ticker}')

if __name__ == '__main__':
    main()