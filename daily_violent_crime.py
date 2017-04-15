'''
Same as daily_crime_counts.py but only consideres 
violent crimes.
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
    .filter(lambda x: ('HOMICIDE' in x[7] or 'MURDER' in x[7] or 'ASSAULT' in x[7] or 'RAPE' in x[7] or 'ROBBERY' in x[7]))\
    .map(lambda x: (day_of_crime(x[1]), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey(ascending=True)

    lines.saveAsTextFile("daily_violent_crime.out")

    sc.stop()

