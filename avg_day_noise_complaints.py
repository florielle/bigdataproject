'''
Average number of noise compaints reported by day of year (between 2010 and 2015)
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
    def day_of_complaint(day):
        try:
            complaint_date = datetime.strptime(day[:10],'%m/%d/%Y')
            monthday = ''.join([str(complaint_date.month),'-',str(complaint_date.day)])
            return monthday
        except:
            return ''

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (x[1], 1))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (day_of_complaint(x[0]), (x[1], 1)))\
    .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
    .map(lambda x: (x[0], float(x[1][0])/x[1][1]))

    lines.saveAsTextFile("avg_day_noise_complaints.out")

    sc.stop()

