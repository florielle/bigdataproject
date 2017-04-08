from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)
    first_line = lines.first()

    if first_line.split(',')[0] == u'CMPLNT_NUM':
        # First line is header
        # Filter header out
        lines = lines.filter(lambda x: x != first_line)

    def valid_check(date):
        try:
            new_date = datetime.strptime(date,'%m/%d/%Y')
            if new_date.year > 1960 and new_date.year < 2017:
                return 'VALID'
            else:
                return 'INVALID'
        except:
            if date == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tDATE\tExact date or start date of occurrence for the reported event\t%s' % (x[1], valid_check(x[1])))

    lines.saveAsTextFile("col1.out")

    sc.stop()
