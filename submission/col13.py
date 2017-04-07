from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_boro(string):
        if string IN ('QUEENS', 'STATEN ISLAND', 'BRONX', 'BROOKLYN', 'MANHATTAN'):
            return 'VALID'
        elif string.replace(' ', '') == '':
            return 'NULL'
        else:
            return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s TEXT nyc borough %s' % (x[13], valid_boro(x[13])))

    lines.saveAsTextFile("col13.out")

    sc.stop()
