'''
It filters by [crime_type] argument and returns the counts
on the number of days enlapsed between occurrence and report to the police. 
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

crime_type = sys.argv[2]

def delta_days(date1, date2):
    try:
        difference = datetime.strptime(date2,'%m/%d/%Y') - datetime.strptime(date1,'%m/%d/%Y')
        return difference.days
    except:
        return ''

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: ((x[7], delta_days(x[1], x[5])),1))\
    .filter(lambda x: x[0][1] != '' and x[0][0] == crime_type)\
    .reduceByKey(lambda x,y: x+y)\

    lines.saveAsTextFile("output.out")

    sc.stop()
