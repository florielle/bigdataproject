from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: ((x[14]),1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey()

    lines.saveAsTextFile("by_county.out")

    sc.stop()
