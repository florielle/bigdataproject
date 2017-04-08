from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_long(num):
        try:
            if float(num) >= -75 and float(num) <= -73:
                return 'VALID'
            else:
                return 'INVALID'    
        except:
            if num == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s FLOAT Longitude coordinate in decimal degrees %s' % (x[22], valid_long(x[22])))

    lines.saveAsTextFile("col22.out")

    sc.stop()
