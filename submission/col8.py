from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_3digits(num):
        if num.isdigit() and len(num) == 3:
            return 'VALID'    
        elif num == '':
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s INT Three digit internal classification code %s' % (x[8], valid_3digits(x[8])))

    lines.saveAsTextFile("col8.out")

    sc.stop()
