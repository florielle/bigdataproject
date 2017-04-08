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

    def valid_lat(num):
        try:
            if float(num) >= 40 and float(num) <= 42:
                return 'VALID'
            else:
                return 'INVALID'    
        except:
            if num == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tFLOAT\tLatitude coordinate in decimal degrees\t%s' % (x[21], valid_lat(x[21])))

    lines.saveAsTextFile("col21.out")

    sc.stop()
