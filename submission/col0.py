from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

# CMPLNT_NUM,Randomly generated persistent ID for each complaint
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
    .map(lambda x: '%s\tINT Randomly generated persistent ID for each complaint\t%s' % (x[0], valid_digit(x[0])))

    lines.saveAsTextFile("col0.out")

    sc.stop()
