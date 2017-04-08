#!/usr/bin/python

# Call:
# hjs -files /home/fnd212/BD/project/bigdataproject/map_reduce/ 
# -mapper map.py -reducer reduce.py -input col0.out 
# -output col0_counts.out


import sys
import csv

# input comes from STDIN (stream data that goes to the program)
for entry in csv.reader(sys.stdin, delimiter='\t'):

    key = entry[-1]
    value = '1'
    print('{0}\t{1}'.format(key, value))
