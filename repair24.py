from __future__ import print_function
import sys
from pyspark import SparkContext
import csv, StringIO
from datetime import datetime
from datetime import timedelta

def fix_date(date):
    try:
        right_date = datetime.strptime(date,'%m/%d/%Y') + timedelta(days=1)
        return datetime.strftime(right_date,'%m/%d/%Y')
    except:
        return ''

def check_time_and_fix(row):
    if row[2] == '24:00:00':
        row[2] = '00:00:00'
        row[1] = fix_date(row[1])
    if row[4] == '24:00:00':
        row[4] = '00:00:00'
        row[3] = fix_date(row[3])
    return row

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = StringIO.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: csv.reader(x))\
   .map(lambda x: check_time_and_fix(x))\
   .map(list_to_csv_str)

    lines.saveAsTextFile("output.out")

    sc.stop()
