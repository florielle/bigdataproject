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
            new_date = datetime.strptime(date,'%m/%d/%Y')
            if new_date.year > 1900 and new_date.year < 2017:
                return 'VALID'
            else:
                return 'INVALID'
        except:
            if date == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s DATE Exact date or start date of occurrence for the reported event %s' % (x[1], valid_check(x[1])))

    lines.saveAsTextFile("col1.out")

    sc.stop()
