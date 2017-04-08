from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from numpy import np

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_string(string):
        try:
            float(string.replace(' ', ''))
            return 'INVALID'
        except:
            if string.replace(' ', '') == '' or np.isnan(string):
                return 'NULL'
            else:
                return 'VALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s TEXT jurisdiction %s' % (x[12], valid_string(x[12])))

    lines.saveAsTextFile("col12.out")

    sc.stop()
