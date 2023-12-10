#!/bin/python3
"""mapper.py
https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
"""

import sys
import re

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip("-_—.,?!\"'`’’“” \t")
    #print("line=", line)
    # split the line into words
    words = line.split(",")
    #print("words=", words)
    # increase counters
    if words[7] != "Russia":
        print( '%s\t%s' % (words[7], 1))

