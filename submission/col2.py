from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def valid_check(time):
    try:
        datetime.strptime(time,'%H:%M:%S')
        return 'VALID'
    except:
        if time == '':
            return 'NULL'
        else:
            return 'INVALID'

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: ('%s DATE Exact date of occurrence for the reported event %s' % (x[2], valid_check(x[2]))))\

    lines.saveAsTextFile("col2.out")

    sc.stop()
