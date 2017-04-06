from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_lat(num):
        try:
            if float(num) >= -90 and float(num) <= 90:
                return 'VALID'
            else:
                return 'INVALID'    
        except:
            if num == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s FLOAT Latitude coordinate in decimal degrees %s' % (x[21], valid_lat(x[21])))

    lines.saveAsTextFile("col21.out")

    sc.stop()
