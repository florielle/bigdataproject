#!/usr/bin/python

# Call:
# hjs -files /home/fnd212/BD/BD_A_01/task4 -mapper task4/map.py \
# -reducer task4/reduce.py -input \
# /user/ecc290/HW1data/parking-violations.csv \
# -output /user/fnd212/task4/task4.out

import sys
import csv

# input comes from STDIN (stream data that goes to the program)
for entry in csv.reader(sys.stdin, delimiter='\t'):

    key = entry[-1]
    value = '1'
    print('{0}\t{1}'.format(key, value))

