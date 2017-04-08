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
    .map(lambda x: ((x[7], (delta_days(x[1], x[5]), 1))))\
    .filter(lambda x: x[1][0] != '')
    
    avg_N = lines.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
    .map(lambda x: (x[0], (float(x[1][0])/x[1][1], x[1][1])))\
    .collectAsMap()

    output = lines.map(lambda x: (x[0], (x[1][0] - avg_N[x[0]][0])**2))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (x[0], avg_N[x[0]][0], (x[1]/float(avg_N[x[0]][1]))**(0.5)))
    
    output.saveAsTextFile("output.out")

    sc.stop()
