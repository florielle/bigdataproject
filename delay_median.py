from __future__ import print_function
import sys
from pyspark import SparkContext
from csv import reader
from datetime import datetime

def median(data_list):
    """
    Finds the median in a list of numbers.
    """
    n = len(data_list)
    data_list.sort()
    # Test whether the n is odd
    if n % 2 == 1:
        # If is is, get the index simply by dividing it in half
        index = int(n / 2) 
        return data_list[index]
    
    else:
        # If the n is even, average the two values at the center
        low_index = int((n / 2) - 1)
        high_index = int(n / 2)
        average = (data_list[low_index] + data_list[high_index]) / 2
        return average

def delta_days(date1, date2):
    try:
        difference = datetime.strptime(date2,'%m/%d/%Y') - datetime.strptime(date1,'%m/%d/%Y')
        return difference.days
    except:
        return ''

if __name__ == "__main__":
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    lines = lines.mapPartitions(lambda x: reader(x))\
    .map(lambda x: (x[7], [delta_days(x[1], x[5])]))\
    .filter(lambda x: x[1][0] != '')\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (x[0], median(x[1])))

    lines.saveAsTextFile("output.out")

    sc.stop()
