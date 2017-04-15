'''
Returns the count of incidents aggregated by 
weekday and weekend.
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    numdays = {'WEEKDAY': 5.,
              'WEEKEND': 2.}

    def weekday_check(date):
        try:
            new_date = datetime.strptime(date,'%m/%d/%Y')
            if new_date.weekday() < 5:
                return 'WEEKDAY'
            else:
                return 'WEEKEND'
        except:
            return ''

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (weekday_check(x[1]), 1))\
    .filter(lambda x: x[0] != '')\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (x[0], x[1]/numdays[x[0]]))

    lines.saveAsTextFile("output.out")

    sc.stop()
