#!/usr/bin/python3

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--path', type=str, default='bigdata.csv', help='File path')
parser.add_argument('--elements', type=int, default=1000000, help='# of elements')
opt = parser.parse_args()


old_file = open(opt.path, 'r')
new_file = open(opt.path.replace('.csv', '_reduced.csv'), 'w')

cnt = opt.elements
while cnt > 0:

	line = old_file.readline()
	new_file.write(line)

	cnt -= 1

	print(cnt)

new_file.close()
print('done')
