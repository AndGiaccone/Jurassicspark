#!/user/bin/python3

import argparse
import datetime
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--input-path', type=str, help='Input file path')
parser.add_argument('--output-path', type=str, help='Output file path')
parser.add_argument('--execution-memory', type=str, default='2048m', help='Spark execution memory')
args = parser.parse_args()

#-------------------------------------------------------------------------------------------------------------------------

def parsing(line):
	try:
		return [(line[0], float(line[2]), datetime.datetime.strptime(line[7], DATE_FORMATTER))]
	except ValueError:
		return []


def cartesian_filter(car_row):
	((ticker1, months1), (ticker2, months2)) = car_row

	if ticker1 >= ticker2:
		return False

	for i in range(12):
		if months1[i] is None or months2[i] is None or abs(months1[i]-months2[i]) > THRESHOLD:
			return False

	return True


def month_map(row):
	month = [None, None, None, None, None, None, None, None, None, None, None, None]
	month[row[0][1]-1] = row[1]

	return (row[0][0], month)


def month_join(x, y):

	res = [None, None, None, None, None, None, None, None, None, None, None, None]
	for i in range(12):
		if x[i] is not None:
			res[i] = x[i]
		elif y[i] is not None:
			res[i] = y[i]

	return res


def output_map(car_row):
	((ticker1, months1), (ticker2, months2)) = car_row
	line = '(%s,%s)' % (ticker1, ticker2)

	for i in range(12):
		line = line + ('\t(%f,%f)' % (months1[i],months2[i]))

	return line


#-------------------------------------------------------------------------------------------------------------------------

spark = SparkSession.builder.appName('Job 3').config('spark.execution.memory', args.execution_memory).getOrCreate()

#-------------------------------------------------------------------------------------------------------------------------

YEAR = 2017
THRESHOLD = 1.0

DATE_FORMATTER = '%Y-%m-%d'

price_rdd = spark.sparkContext.textFile(args.input_path).map(f = lambda line: line.strip().split('\t')).filter(lambda row: row[7][0:4] == '2017').flatMap(f = parsing)
price_rdd = price_rdd.map(f = lambda row: ((row[0], row[2].month), (row[1],row[2])))

mindate_rdd = price_rdd.reduceByKey(func = lambda x,y: x if x[1] < y[1] else y)
maxdate_rdd = price_rdd.reduceByKey(func = lambda x,y: x if x[1] > y[1] else y)

del price_rdd

varclose_rdd = mindate_rdd.union(maxdate_rdd).reduceByKey(func = lambda x,y: ((x[0]-y[0])/y[0])*100 if x[1]>y[1] else ((y[0]-x[0])/x[0])*100)

del mindate_rdd
del maxdate_rdd

varclose_rdd = varclose_rdd.map(f = month_map).reduceByKey(func = month_join)
varclose_rdd = varclose_rdd.cartesian(varclose_rdd).filter(cartesian_filter).map(f = output_map).saveAsTextFile(args.output_path)

"""
[ticker, close, date]

(ticker, (ticker, varClose1, ..., varClose12))


((ticker, month), (close, date)) -> min/max -> varClose = ((ticker, month), varClose) -> (month, (ticker, varClose))


((ticker1, ticker2), (month, varClose1, varClose2))

"""
