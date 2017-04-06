from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
    column = int(sys.argv[2])

    def valid_or_not(date):
        try:
            datetime.strptime(date,'%m/%d/%Y')
            return 'Valid'
        except:
            return date

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (valid_or_not(str(x[column])), 1))\
    .reduceByKey(lambda x,y: x+y)

    lines.saveAsTextFile("valid_dates.out")

    sc.stop()

