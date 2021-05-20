#!/usr/bin/python3

import sys
import datetime

YEAR = 2017
DATE_FORMATTER = '%Y-%m-%d'

THRESHOLD = 1.0

tickers = {
	1 : {},
	2 : {},
	3 : {},
	4 : {},
	5 : {},
	6 : {},
	7 : {},
	8 : {},
	9 : {},
	10 : {},
	11 : {},
	12 : {}
}

for line in sys.stdin:

	ticker, close, date = line.strip().split('\t')

	try:
		date = datetime.datetime.strptime(date, DATE_FORMATTER)
		if date.year != 2017:
			continue
		close = float(close)
	except ValueError:
		continue
		
	if ticker not in tickers[date.month]:
		tickers[date.month][ticker] = {
			'minDate' : {
				'day' : date,
				'close' : close
			},
			'maxDate' : {
				'day' : date,
				'close' : close
			},
			'avgClose' : 0.0
		}
	else:
		if tickers[date.month][ticker]['minDate']['day'] > date:
			tickers[date.month][ticker]['minDate']['day'] = date
			tickers[date.month][ticker]['minDate']['close'] = close
			tickers[date.month][ticker]['avgClose'] = ((tickers[date.month][ticker]['maxDate']['close']-tickers[date.month][ticker]['minDate']['close'])/tickers[date.month][ticker]['minDate']['close'])*100
			
		elif tickers[date.month][ticker]['maxDate']['day'] < date:
			tickers[date.month][ticker]['maxDate']['day'] = date
			tickers[date.month][ticker]['maxDate']['close'] = close
			tickers[date.month][ticker]['avgClose'] = ((tickers[date.month][ticker]['maxDate']['close']-tickers[date.month][ticker]['minDate']['close'])/tickers[date.month][ticker]['minDate']['close'])*100			

#chore: removing date information for reducing RAM usage
for month in range(1,13):
	for ticker in tickers[month]:
		del tickers[month][ticker]['minDate']
		del tickers[month][ticker]['maxDate']

couples = []

#first month: adding all possible couples
first_month_sorted_tickers = sorted(tickers[1], key=lambda el : tickers[1][el]['avgClose'])

for i in range(len(first_month_sorted_tickers)-1):
	j = i+1
	while j < len(first_month_sorted_tickers):
		if abs(tickers[1].get(first_month_sorted_tickers[i])['avgClose']-tickers[1].get(first_month_sorted_tickers[j])['avgClose']) <= THRESHOLD:
			couples.append( (first_month_sorted_tickers[i], first_month_sorted_tickers[j]) )
			j += 1
		else:
			break

#after first month: delete the couples that not respect anymore the threshold
for couple in couples:
	(i, j) = couple
	removed = False
	for month in range(2,13):
		try:
			if abs(tickers[month].get(i)['avgClose']-tickers[month].get(j)['avgClose']) > THRESHOLD:
				removed = True
				break
		except TypeError:
				removed = True
				break
				
	if removed == False:
		names = '(%s, %s)' % (i, j)
		gen = '(%f, %f)' % (tickers[1].get(i)['avgClose'], tickers[1].get(j)['avgClose'])
		feb = '(%f, %f)' % (tickers[2].get(i)['avgClose'], tickers[2].get(j)['avgClose'])
		mar = '(%f, %f)' % (tickers[3].get(i)['avgClose'], tickers[3].get(j)['avgClose'])
		apr = '(%f, %f)' % (tickers[4].get(i)['avgClose'], tickers[4].get(j)['avgClose'])
		may = '(%f, %f)' % (tickers[5].get(i)['avgClose'], tickers[5].get(j)['avgClose'])
		jun = '(%f, %f)' % (tickers[6].get(i)['avgClose'], tickers[6].get(j)['avgClose'])
		jul = '(%f, %f)' % (tickers[7].get(i)['avgClose'], tickers[7].get(j)['avgClose'])
		aug = '(%f, %f)' % (tickers[8].get(i)['avgClose'], tickers[8].get(j)['avgClose'])
		sep = '(%f, %f)' % (tickers[9].get(i)['avgClose'], tickers[9].get(j)['avgClose'])
		ocb = '(%f, %f)' % (tickers[10].get(i)['avgClose'], tickers[10].get(j)['avgClose'])
		nov = '(%f, %f)' % (tickers[11].get(i)['avgClose'], tickers[11].get(j)['avgClose'])
		dec = '(%f, %f)' % (tickers[12].get(i)['avgClose'], tickers[12].get(j)['avgClose'])
		
		print(names, gen, feb, mar, apr, may, jun, jul, aug, sep, ocb, nov, dec, sep='\t')
