#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 20 14:09:47 2018

@author: Thomas
"""

#!/usr/bin/env python

import requests
import pytz
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime
from pandas_datareader import DataReader


SITE = "https://www.slickcharts.com/sp500" # Wikipedia server avoir scrapping
START = datetime(1990, 1, 1, 0, 0, 0, 0, pytz.utc)
END = datetime.today().utcnow()


def scrape_list(site):
    page = requests.get(site).text #Import all html data as text
    soup = BeautifulSoup(page,'lxml')

    table = soup.find('table', {'class': 'table table-hover table-borderless table-sm'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[0].string.strip())
            ticker = str(col[2].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers


def download_ohlc(sector_tickers, start, end):
    sector_ohlc = {}
    for tickers in sector_tickers.items():
        print('Downloading data from Yahoo for %s tickers' % tickers)
        data = DataReader(tickers, 'google', start, end)
        for item in ['Open', 'High', 'Low']:
            data[item] = data[item] * data['Adj Close'] / data['Close']
        data.rename(items={'Open': 'open', 'High': 'high', 'Low': 'low',
                           'Adj Close': 'close', 'Volume': 'volume'},
                    inplace=True)
        data.drop(['Close'], inplace=True)
        sector_ohlc[sector] = data
    print('Finished downloading data')
    return sector_ohlc


def store_HDF5(sector_ohlc, path):
    with pd.get_store(path) as store:
        for sector, ohlc in sector_ohlc.iteritems():
            store[sector] = ohlc


def get_snp500():
    sector_tickers = scrape_list(SITE)
    sector_ohlc = download_ohlc(sector_tickers, START, END)
    store_HDF5(sector_ohlc, 'snp500.h5')


if __name__ == '__main__':
    get_snp500()