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

    def valid_check(date, end_date):
        try:
            new_date = datetime.strptime(date,'%m/%d/%Y')
            if new_date.year >= 1900 and new_date.year < 2017:
                try:
                    # Ensure that the end date is greater or equal the start date
                    new_end_date = datetime.strptime(end_date,'%m/%d/%Y')
                    if new_end_date >= new_date:
                        return 'VALID'
                    else:
                        return 'INVALID'
                except:
                    # If no end date, assume valid
                    return 'VALID'
            else:
                # If cannot cast to datetime is invalid
                return 'INVALID'
        except:
            if date == '':
                return 'NULL'
            else:
                return 'INVALID'

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: '%s\tDATE\tExact date or start date of occurrence for the reported event\t%s' % (x[1], valid_check(x[1], x[3])))

    lines.saveAsTextFile("col1.out")

    sc.stop()
