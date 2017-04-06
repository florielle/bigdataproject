from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_digit(num):
        if num.isdigit():
            return 'VALID'    
        elif num == '':
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s INT Precinct code for where the incident occurred %s' % (x[14], valid_digit(x[14])))

    lines.saveAsTextFile("col14.out")

    sc.stop()
