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


    def valid_3digits(num):
        if num.isdigit() and len(num) == 3:
            return 'VALID'    
        elif num == '':
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tINT\tThree digit offense classification code\t%s' % (x[6], valid_3digits(x[6])))

    lines.saveAsTextFile("col6.out")

    sc.stop()
