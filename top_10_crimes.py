'''
Return the top ten crimes in terms of nomber of 
occurrences
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (x[9], 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: -x[1])

    sc.parallelize(lines.take(10))\
    .saveAsTextFile("top_10_crimes.out")

    sc.stop()

