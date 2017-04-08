from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
    first_line = lines.first()

    if first_line.split(',')[0] == u'CMPLNT_NUM':
        # First line is header
        # Filter header out
        lines = lines.filter(lambda x: x != first_line)

    def valid_y_coord(num):
        try:
            if float(num)>=110626 and float(num)<=424499:
                return 'VALID'
            else:
                return 'INVALID'
        elif num == '':
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tFLOAT\tY coord for NYS plane system\t%s' % (x[20], valid_y_coord(x[20])))

    lines.saveAsTextFile("col20.out")

    sc.stop()
