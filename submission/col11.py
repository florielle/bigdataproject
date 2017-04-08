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

    def valid_code(string):
        if string in ('MISDEMEANOR', 'FELONY', 'VIOLATION'):
            return 'VALID'
        elif not string.replace(' ', ''):
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tTEXT\tlevel of offense\t%s' % (x[11], valid_code(x[11])))

    lines.saveAsTextFile("col11.out")

    sc.stop()
