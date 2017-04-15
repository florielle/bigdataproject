'''
Same as byprecinct.py but consider only the 
violent crimes. 
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader



if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .filter(lambda x: x != '')\
    .filter(lambda x: ('HOMICIDE' in x[7] or 'MURDER' in x[7] or 'ASSAULT' in x[7] or 'RAPE' in x[7] or 'ROBBERY' in x[7]))\
    .map(lambda x: ((x[14]),1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortByKey()

    lines.saveAsTextFile("by_precinct_violent.out")

    sc.stop()
