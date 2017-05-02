'''
Returns the average temperature for each day
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    def parse_date(date):
        try:
            day = datetime.strptime(date,'%Y-%m-%d')
            return datetime.strftime(day, '%m/%d/%Y')
        except:
            return ''
    
    def parse_rain(precip):
        try:
            return float(precip.strip(" in"))
        except:
            return 0

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x, delimiter="\t"))\
    .map(lambda x: (parse_date(x[1]), parse_rain(x[-3])))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: "%s\t%s" % (x[0],x[1]))

    lines.saveAsTextFile("output.out")

    sc.stop()
