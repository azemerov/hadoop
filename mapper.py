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
    # split the line into words
    words = re.split("-|_|—|\.|,|\?|!|\"|'|`|’|’|“| |\t", line) #line.split("-_—.,?!\"'`’’“” \t")
    #words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print( '%s\t%s' % (word, 1))

