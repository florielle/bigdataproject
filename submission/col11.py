from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
import numpy as np

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_code(string):
        if string in ('ATTEMPTED', 'COMPLETED'):
            return 'VALID'
        elif string.replace(' ', '') == '' or np.isnan(string):
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s TEXT completed or attempted status %s' % (x[10], valid_code(x[10])))

    lines.saveAsTextFile("col10.out")

    sc.stop()
