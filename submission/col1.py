from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    def valid_check(date):
        try:
            datetime.strptime(date,'%m/%d/%Y')
            return 'VALID'
        except:
            if date == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s DATE Exact date or start date of occurrence for the reported event %s' % (x[1], valid_check(x[1])))

    lines.saveAsTextFile("col1.out")

    sc.stop()
