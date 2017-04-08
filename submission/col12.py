from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_string(string):
        chars = set('qwertyuiopasdfghjklzxcvbnm')
        try:
            if any((c in chars) for c in string.lower()):
                return 'VALID'
            else:
                return 'INVALID'
        except:
            if string.replace(' ', '') == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s TEXT jurisdiction %s' % (x[12], valid_string(x[12])))

    lines.saveAsTextFile("col12.out")

    sc.stop()
