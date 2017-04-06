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
    .map(lambda x: '%s DATE Ending date of occurrence for the reported event %s' % (x[3], valid_check(x[3])))

    lines.saveAsTextFile("col3.out")

    sc.stop()
