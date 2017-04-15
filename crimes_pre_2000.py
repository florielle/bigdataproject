'''
Only considers crimes that happen previous to the year 2000
and returns a sorted count of the descriptions of the crimes.
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    def year_of_date(date):
        try:
            return datetime.strptime(date,'%m/%d/%Y').year
        except:
            return ''


    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: year_of_date(x[1])<2000)\
    .map(lambda x: (x[7], 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: -x[1])

    lines.saveAsTextFile("top_crimes_pre_2000.out")

    sc.stop()

