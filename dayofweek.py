from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def dayofweek(date):
        try:
            return datetime.strptime(date,'%m/%d/%Y').weekday()
        except:
            return ''

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (dayofweek(x[1]), 1))\
    .filter(lambda x: x[0] != '')\
    .reduceByKey(lambda x,y: x+y)\

    lines.saveAsTextFile("output.out")

    sc.stop()
