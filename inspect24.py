from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def valid_or_not(time):
    try:
        datetime.strptime(time,'%H:%M:%S')
        return 'Valid'
    except:
        return time

def delta_days(date1, date2):
    difference = datetime.strptime(date2,'%m/%d/%Y') - datetime.strptime(date1,'%m/%d/%Y')
    return difference.days

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (valid_or_not(str(x[2])), (x[1], x[2], x[3], x[4])))\
    .filter(lambda x: x[0] == '24:00:00' and x[1][0] != '' and  x[1][2] != '')\
    .map(lambda x: (delta_days(x[1][0], x[1][2]), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey()

    lines.saveAsTextFile("output.out")

    sc.stop()
