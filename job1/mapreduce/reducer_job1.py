#!/usr/bin/python3

import sys
import datetime

DATE_FORMATTER = '%Y-%m-%d'

tickers = {}

for line in sys.stdin:

    line = line.strip()

    ticker, close, low, high, day = line.split('\t')

    try:
        close = float(close)
        low = float(low)
        high = float(high)
        day = datetime.datetime.strptime(day, DATE_FORMATTER)
    except ValueError:
        continue

    if ticker not in tickers:
        tickers[ticker] = {
            'minDate' : {
                'day' : day,
                'closeValue' : close
            },
            'maxDate' : {
                'day' : day,
                'closeValue' : close
            },
            'price' : {
                'low' : low,
                'high' : high
            },
            'avgClose' : 0.0
        }

    else:
        if tickers[ticker]['minDate']['day'] > day:
            tickers[ticker]['minDate']['day'] = day
            tickers[ticker]['minDate']['closeValue'] = close
            tickers[ticker]['avgClose'] = ((tickers[ticker]['maxDate']['closeValue']-tickers[ticker]['minDate']['closeValue'])/tickers[ticker]['minDate']['closeValue'])*100

        elif tickers[ticker]['maxDate']['day'] < day:
            tickers[ticker]['maxDate']['day'] = day
            tickers[ticker]['maxDate']['closeValue'] = close
            tickers[ticker]['avgClose'] = ((tickers[ticker]['maxDate']['closeValue']-tickers[ticker]['minDate']['closeValue'])/tickers[ticker]['minDate']['closeValue'])*100

        if tickers[ticker]['price']['low'] >= low:
            tickers[ticker]['price']['low'] = low

        if tickers[ticker]['price']['high'] <= high:
            tickers[ticker]['price']['high'] = high

for ticker in tickers:
    minDate = tickers[ticker]['minDate']['day'].strftime(DATE_FORMATTER)
    maxDate = tickers[ticker]['maxDate']['day'].strftime(DATE_FORMATTER)

    low = tickers[ticker]['price']['low']
    high = tickers[ticker]['price']['high']

    print('%s\t%s\t%s\t%s\t%f\t%f' % (ticker, minDate, maxDate, tickers[ticker]['avgClose'], low, high)) 
