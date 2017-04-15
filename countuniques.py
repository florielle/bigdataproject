'''
Returns the count of each unique value in the column.
'''

from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

column = int(sys.argv[2])

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
       first_line = lines.first()

    if first_line.split(',')[0] == u'CMPLNT_NUM':
        # First line is header
        # Filter header out
        lines = lines.filter(lambda x: x != first_line)


    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (x[column],1))\
    .reduceByKey(lambda x,y: x+y)

    outfile = "uniquecounts"+str(column)+".out" 

    lines.saveAsTextFile(outfile)

    sc.stop()
