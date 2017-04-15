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
            crime_date = datetime.strptime(day,'%m/%d/%Y')
            year_crime = crime_date.year
            monthday = ''.join([str(crime_date.month),'-',str(crime_date.day)])
            if year_crime>= 2008:
              return monthday
            else:
                return ''
        except:
            return ''

    lines = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: ('IMPAIRED DRIVING' in x[7]))\
    .map(lambda x: (x[1], 1))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (day_of_crime(x[0]), (x[1], 1)))\
    .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
    .map(lambda x: (x[0], float(x[1][0])/x[1][1]))

    lines.saveAsTextFile("avg_day_DUI_crime.out")

    sc.stop()

