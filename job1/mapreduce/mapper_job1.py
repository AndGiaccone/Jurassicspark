#!/usr/bin/python3
"""mapperA.py"""

import sys

for line in sys.stdin:
    line = line.strip()
    
    ticker, _, close, _, low, high, _, day = line.split('\t')

    print(ticker,close,low,high,day, sep='\t')
