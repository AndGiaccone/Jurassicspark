#!/usr/bin/python3

import sys

for line in sys.stdin:
	splitted = line.strip().split('\t')

	if len(splitted) == 8:
		ticker, open_price, closed_price, _, low_price, high_price, volume, date = splitted
		if int(date[0:4]) >= 2009:
			print(ticker, open_price, closed_price, low_price, high_price, volume, date, sep='\t')
		
	elif len(splitted) == 5:
		ticker, _, name, sector, industry = splitted
		print('..', ticker, sector, industry, name, sep='\t')
