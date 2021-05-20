#!/usr/bin/python3

import sys

for line in sys.stdin:
	ticker, _, close, _, _, _, _, date = line.strip().split('\t')
	
	if date[0:4] == '2017':
		print(ticker, close, date, sep='\t')
