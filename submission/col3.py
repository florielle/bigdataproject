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

    def valid_check(date, start_date):
        try:
            new_date = datetime.strptime(date, '%m/%d/%Y')
            if new_date.year >= 1900 and new_date.year < 2017:
                try:
                    # Ensure that the end date is greater or equal the start date
                    new_start_date = datetime.strptime(start_date,'%m/%d/%Y')
                    if new_start_date <= new_date:
                        return 'VALID'
                    else:
                        return 'INVALID'
                except:
                    # If no start date, assume valid
                    return 'VALID'

                return 'VALID'
            else:
                return 'INVALID'
        except:
            if date == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tDATE\tEnding date of occurrence for the reported event\t%s' % (x[3], valid_check(x[3], x[1])))

    lines.saveAsTextFile("col3.out")

    sc.stop()
