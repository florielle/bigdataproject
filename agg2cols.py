from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

column1 = int(sys.argv[2])
column2 = int(sys.argv[3])

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: ((x[column1],x[column2]),1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey()

    lines.saveAsTextFile("output.out")

    sc.stop()
