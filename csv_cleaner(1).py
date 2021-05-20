#!/usr/bin/python3

import argparse
import html

parser = argparse.ArgumentParser()
parser.add_argument('--path', type=str, default='bigdata.csv', help='File path')
opt = parser.parse_args()


old_file = open(opt.path, 'r')
new_file = open(opt.path.replace('.csv', '_new.csv'), 'w')

cnt = 0
while True:

	line = old_file.readline()

	if not line:
		break

	if ',"' in line and '",' in line:
		print('Line {} corrected'.format(cnt))
		name = line[line.find(',"')+2 : line.find('",')]
		parsed_name = name.replace(',', '~')
		line = line.replace('"'+name+'"', parsed_name)

	line = line.replace(',','\t')
	
	if '^' in line.split('\t')[0] or '.' in line.split('\t')[0]:
	    continue
	
	line = line.replace('~',',')

	new_file.write(html.unescape(line))

	cnt += 1

	print(cnt)

new_file.close()
print('done')
