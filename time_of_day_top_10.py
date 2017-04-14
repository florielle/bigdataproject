from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
    def hour_of_crime(time):
        try:
            crime_time = datetime.strptime(time,'%H:%M:%S').hour
            return crime_time
        except:
            return ''

    top_lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (x[9], 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda x: -x[1])\
    .map(lambda x: (x[0]))

    top_ten=top_lines.take(10)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: x[9] in top_ten)\
    .map(lambda x: (hour_of_crime(x[2]), 1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey(ascending=True)

    lines.saveAsTextFile("time_of_crime_top_10.out")

    sc.stop()

