#!/usr/bin/python3

import sys
import datetime

meta = {}
rows = {}


for line in sys.stdin:
	line = line.strip()
	splitted = line.split('\t')
	
	if splitted[0] == '..':
		meta[splitted[1]] = {
			'sector' : splitted[2],
			'industry' : splitted[3],
			'name' : splitted[4]
		}
	else:
		try:
			print(splitted[0], meta.get(splitted[0])['name'], meta.get(splitted[0])['industry'], meta.get(splitted[0])['sector'], splitted[1], splitted[2], splitted[3], splitted[4], splitted[5], splitted[6], sep='\t')
		except TypeError:
			continue
