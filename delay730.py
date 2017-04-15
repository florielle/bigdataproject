'''
Filters those incidents which were reported between 720 and 740
days after occurrence. Returns the counts aggregated by type of crime. 
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

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
    .filter(lambda x: x[0][1] != '')\
    .filter(lambda x: int(x[0][1]) < 740 and int(x[0][1]) > 720)\
    .reduceByKey(lambda x,y: x+y)\

    lines.saveAsTextFile("output.out")

    sc.stop()
