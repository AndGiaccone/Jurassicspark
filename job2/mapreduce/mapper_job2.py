#!/usr/bin/python3

import sys

for line in sys.stdin:

	splitted = line.strip().split('\t')

	if len(splitted) == 5:
		ticker, _, _, sector, _ = splitted
		print('..', ticker, sector, sep='\t')

	elif len(splitted) == 8:
		ticker, _, close, _, _, _, volume, day = splitted

		if int(day[0:4]) >= 2009:
			print(ticker, day, close, volume, sep='\t')
