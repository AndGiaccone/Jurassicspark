#!/usr/bin/python3

import sys
import datetime

DATE_FORMATTER = '%Y-%m-%d'

sectors = {}

ticker_sector = {}

"""

sectors = {
	'sector' : {
		'2009' : {
			'ABC' : {
				'minDay' : {
					'day' : datetime,
					'close' : float
				},
				'maxDay' : {
					'day' : datetime,
					'close' : float
				},
				'volume' : long
			},
			'XYZ' : ...
		},
		'2010' : ...
	}
}

"""

for line in sys.stdin:

	splitted = line.split('\t')

	if len(splitted) == 3 and splitted[0] == '..':
		ticker_sector[splitted[1]] = splitted[2]
		continue

	sector, ticker, day, close, volume = ticker_sector.get(splitted[0]), splitted[0], splitted[1], splitted[2], splitted[3]

	try:
		day = datetime.datetime.strptime(day, DATE_FORMATTER)
		close = float(close)
		volume = int(volume)
	except ValueError:
		continue

	if sector not in sectors:
		sectors[sector] = {}

	if day.year not in sectors[sector]:
		sectors[sector][day.year] = {}

	if ticker not in sectors[sector][day.year]:
		sectors[sector][day.year][ticker] = {
			'minDate' : {
				'day' : day,
				'closeValue' : close
			},
			'maxDate' : {
				'day' : day,
				'closeValue' : close
			},
			'volume' : volume
		}
	else:
		sectors[sector][day.year][ticker]['volume'] += volume

		if sectors[sector][day.year][ticker]['minDate']['day'] > day:
			sectors[sector][day.year][ticker]['minDate']['day'] = day
			sectors[sector][day.year][ticker]['minDate']['closeValue'] = close

		elif sectors[sector][day.year][ticker]['maxDate']['day'] < day:
			sectors[sector][day.year][ticker]['maxDate']['day'] = day
			sectors[sector][day.year][ticker]['maxDate']['closeValue'] = close


for sector in sectors:

	for year in sectors[sector]:

		min_date_sector_value = 0
		max_date_sector_value = 0

		best_avg_close = {
			'ticker' : 'n/a',
			'avgClose' : 0.0
		}

		best_volume = {
			'ticker' : 'n/a',
			'volume' : 0
		}

		for ticker in sectors[sector][year]:
			min_date_sector_value += sectors[sector][year][ticker]['minDate']['closeValue']
			max_date_sector_value += sectors[sector][year][ticker]['maxDate']['closeValue']

			if sectors[sector][year][ticker]['volume'] > best_volume['volume']:
				best_volume['ticker'] = ticker
				best_volume['volume'] = sectors[sector][year][ticker]['volume']

			avg_close = ((sectors[sector][year][ticker]['maxDate']['closeValue']-sectors[sector][year][ticker]['minDate']['closeValue'])/sectors[sector][year][ticker]['minDate']['closeValue'])*100

			if best_avg_close['avgClose'] < avg_close:
				best_avg_close['ticker'] = ticker
				best_avg_close['avgClose'] = avg_close

		avg_sector_close = ((max_date_sector_value-min_date_sector_value)/min_date_sector_value)*100

		print(sector, year, avg_sector_close, best_avg_close['ticker'], best_avg_close['avgClose'], best_volume['ticker'], best_volume['volume'], sep='\t')
