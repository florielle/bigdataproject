'''
Aggregates by the precinct in which the incident occurred
and computes the total count per precinct.
'''
from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader

read_file = sys.argv[1]
out_file = sys.argv[2]

def get_year(x):
    x = x.split('\t')
    if x[3] != 'VALID':
        return ('NULL', 'NULL')
    date = x[0]
    year = int(date[-4:])
    return (year, 1)


if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(read_file)

    years = lines.map(lambda x: get_year(x))\
        .filter(lambda x: x[0] != 'NULL')
        .map(lambda x: '{0}\t{1}'.format(x[0], x[1]))
    

    years.saveAsTextFile(out_file)

    sc.stop()
