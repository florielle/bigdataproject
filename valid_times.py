from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

column = int(sys.argv[2])

def valid_or_not(time):
    try:
        datetime.strptime(time,'%H:%M:%S')
        return 'Valid'
    except:
        return time

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (valid_or_not(x[column]), 1))\
    .reduceByKey(lambda x,y: x+y)

    lines.saveAsTextFile("output.out")

    sc.stop()
