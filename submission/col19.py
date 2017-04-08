from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_x_coord(num):
        try:
            if float(num)>=909126 and float(num)<=1610216:
                return 'VALID'
            else:
                return 'INVALID'
        elif num == '':
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s X coord for NYS plane system %s' % (x[19], valid_x_coord(x[19])))

    lines.saveAsTextFile("col19.out")

    sc.stop()
