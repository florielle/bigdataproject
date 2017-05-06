'''
Aggregates by the precinct in which the incident occurred
and computes the total count per precinct.
'''
from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    by_year_precinct = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: x != '')\
    .map(lambda x: ((x[1][-4:], x[14]),1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey()\
    .map(lambda x: '{0}\t{1}\t{2}'.format(x[0][0], x[0][1], x[1]))

    by_year_precinct.saveAsTextFile("by_precinct_year.out")

    sc.stop()
