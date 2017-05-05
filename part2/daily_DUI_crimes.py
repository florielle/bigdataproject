'''
Returns the count of the number of DUI crimes 
aggregated by date.
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
    def day_of_crime(day):
        try:
            datetime.strptime(day,'%m/%d/%Y')
            return day
        except:
            return ''

    lines = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: ('IMPAIRED DRIVING' in x[7]))\
    .map(lambda x: (day_of_crime(x[1]), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey(ascending=True)

    lines.saveAsTextFile("daily_DUI.out")

    sc.stop()

