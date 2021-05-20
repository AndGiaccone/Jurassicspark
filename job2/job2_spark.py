#!/usr/bin/python3

import argparse
import datetime
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--input-path-price', type=str, help='Input file path')
parser.add_argument('--input-path-meta', type=str, help='Input file path')
parser.add_argument('--output-path', type=str, help='Output file path')
parser.add_argument('--execution-memory', type=str, default='2048m', help='Spark execution memory')
args = parser.parse_args()

#-------------------------------------------------------------------------------------------------------------------------

# (sector, close, volume, date) 

DATE_FORMATTER = '%Y-%m-%d'

def parse_and_map(line):
	try:
		splitted = line.strip().split('\t')
		if 2009 <= int(splitted[7][0:4]) <= 2018:
			return [(splitted[0], (float(splitted[2]), int(splitted[6]), datetime.datetime.strptime(splitted[7], DATE_FORMATTER)) )]
		else:
			return []
	except ValueError:
		return []

def final_reducer(x, y):

	res = [None, None, None, None, None]

	for i in range(5):
		if x[i] is not None:
			res[i] = x[i]
		elif y[i] is not None:
			res[i] = y[i]

	return tuple(res)

#-------------------------------------------------------------------------------------------------------------------------

spark = SparkSession.builder.appName('Job 2').config('spark.execution.memory', args.execution_memory).getOrCreate()

#-------------------------------------------------------------------------------------------------------------------------

price_rdd = spark.sparkContext.textFile(args.input_path_price).flatMap(f = parse_and_map)

sector_rdd = spark.sparkContext.textFile(args.input_path_meta) \
	.map(f = lambda line : line.split('\t')) \
	.map(f = lambda line: (line[0], line[3]))

price_rdd = price_rdd.join(sector_rdd)
del sector_rdd


volume_rdd = price_rdd.map(f = lambda row: ((row[0], row[1][1], row[1][0][2].year), row[1][0][1])) \
	.reduceByKey(func = lambda x,y: x+y) \
	.map(f = lambda row: ((row[0][1], row[0][2]), (row[0][0], row[1])) ) \
	.reduceByKey(func = lambda x,y: x if x[1] > y[1] else y)

xdate_rdd = price_rdd.map(f = lambda row: ((row[0], row[1][1], row[1][0][2].year), (row[1][0][0], row[1][0][2])) )


del price_rdd

mindate_rdd = xdate_rdd.reduceByKey(func = lambda x,y: x if x[1] < y[1] else y)
maxdate_rdd = xdate_rdd.reduceByKey(func = lambda x,y: x if x[1] > y[1] else y)

del xdate_rdd


varclose_rdd = mindate_rdd.union(maxdate_rdd) \
	.reduceByKey(func = lambda x,y: ((x[0]-y[0])/y[0])*100 if x[1] > y[1] else ((y[0]-x[0])/x[0])*100) \
	.map(f = lambda row: ((row[0][1], row[0][2]), (row[0][0], row[1])) ) \
	.reduceByKey(func = lambda x,y: x if x[1] > y[1] else y)


mindate_sector_rdd = mindate_rdd.map(f = lambda row: ((row[0][1], row[0][2]), row[1])).reduceByKey(func = lambda x,y: x+y)
maxdate_sector_rdd = maxdate_rdd.map(f = lambda row: ((row[0][1], row[0][2]), row[1])).reduceByKey(func = lambda x,y: x+y)

varclose_sector_rdd = mindate_sector_rdd.union(maxdate_sector_rdd).reduceByKey(func = lambda x,y: ((x[0]-y[0])/y[0])*100 if x[1] > y[1] else ((y[0]-x[0])/x[0])*100)

del mindate_rdd
del maxdate_rdd

varclose_rdd = varclose_rdd.map(f = lambda row: (row[0], (None, row[1][0], row[1][1], None, None)) )
volume_rdd = volume_rdd.map(f = lambda row: (row[0], (None, None, None, row[1][0], row[1][1])) )
varclose_sector_rdd = varclose_sector_rdd.map(f = lambda row: (row[0], (row[1], None, None, None, None)) )

varclose_rdd.union(volume_rdd) \
	.union(varclose_sector_rdd) \
	.reduceByKey(func = final_reducer) \
	.map(f = lambda row: '{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(row[0][0], row[0][1], row[1][0], row[1][1], row[1][2], row[1][3], row[1][4]) ) \
	.saveAsTextFile(args.output_path)