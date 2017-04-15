'''
Returns the duplicated keys in the primary key.
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
    .map(lambda x: (x[0], 1))\
    .reduceByKey(lambda x,y: x+y)\
    .filter(lambda x: (x[1]>1))

    lines.saveAsTextFile("duplicate_keys.out")

    sc.stop()

