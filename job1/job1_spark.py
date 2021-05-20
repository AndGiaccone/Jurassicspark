#!/usr/bin/python3

import argparse
import datetime
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--input-path', type=str, help='Input file path')
parser.add_argument('--output-path', type=str, help='Output file path')
parser.add_argument('--execution-memory', type=str, default='2048m', help='Spark execution memory')
args = parser.parse_args()

#-----------------------------------------------------------------

DATE_FORMATTER = '%Y-%m-%d'

def parse_and_map(line):
	try:
		splitted = line.strip().split('\t')

		return [(splitted[0], (float(splitted[2]), float(splitted[4]), float(splitted[5]), datetime.datetime.strptime(splitted[7], DATE_FORMATTER)) )]
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

#-----------------------------------------------------------------

spark = SparkSession.builder.appName('Job 1').config('spark.execution.memory', args.execution_memory).getOrCreate()

rdd = spark.sparkContext.textFile(args.input_path).flatMap(f = parse_and_map)

low_rdd = rdd.reduceByKey(func = lambda x,y: x if x[1] < y[1] else y).map(f = lambda x: (x[0], (None, None, None, x[1][1], None)))
high_rdd = rdd.reduceByKey(func = lambda x,y: x if x[1] > y[1] else y).map(f = lambda x: (x[0], (None, None, None, None, x[1][1])))

mindate_rdd = rdd.reduceByKey(func = lambda x,y: x if x[3] < y[3] else y)
maxdate_rdd = rdd.reduceByKey(func = lambda x,y: x if x[3] > y[3] else y)

del rdd

varclose_rdd = mindate_rdd.union(maxdate_rdd) \
	.reduceByKey(func = lambda x,y: ( ((x[0]-y[0])/y[0])*100 if y[3]<x[3] else ((y[0]-x[0])/x[0])*100, None, None, None,None))

mindate_rdd = mindate_rdd.map(f = lambda x: (x[0], (None, x[1][3], None, None, None)))
maxdate_rdd = maxdate_rdd.map(f = lambda x: (x[0], (None, None, x[1][3], None, None)))

low_rdd.union(high_rdd) \
	.union(mindate_rdd) \
	.union(maxdate_rdd) \
	.union(varclose_rdd) \
	.reduceByKey(func = final_reducer).map(f = lambda row: '%s\t%f\t%s\t%s\t%f\t%f' % (row[0], row[1][0], row[1][1].strftime(DATE_FORMATTER), row[1][2].strftime(DATE_FORMATTER), row[1][3], row[1][4])) \
	.saveAsTextFile(args.output_path)