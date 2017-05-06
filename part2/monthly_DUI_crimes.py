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
    def month_year(day):
        try:
            m=datetime.strptime(day,'%m/%d/%Y').month
            y=datetime.strptime(day,'%m/%d/%Y').year
            if y>= 2010:
                return str(m)+"-"+str(y)
            else:
                return ''
        except:
            return ''

    lines = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: ('IMPAIRED DRIVING' in x[7]))\
    .map(lambda x: (month_year(x[1]), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey(ascending=True)

    lines.saveAsTextFile("monthly_DUI.out")

    sc.stop()

