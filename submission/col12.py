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

    def valid_string(string):
        chars = set('qwertyuiopasdfghjklzxcvbnm')
        if any((c in chars) for c in string.lower()):
            return 'VALID'
        elif not string.replace(' ', ''):
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tTEXT\tJurisdiction\t%s' % (x[12], valid_string(x[12])))

    lines.saveAsTextFile("col12.out")

    sc.stop()
