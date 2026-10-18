#!/usr/bin/env python3
"""MapReduce job 1 - reducer (also used as combiner): sums counts per word.

Hadoop sorts mapper output by key, so all lines for one word arrive consecutively.
"""
import sys

current, total = None, 0
for line in sys.stdin:
    word, _, count = line.rstrip("\n").partition("\t")
    if word != current:
        if current is not None:
            print(f"{current}\t{total}")
        current, total = word, 0
    total += int(count)
if current is not None:
    print(f"{current}\t{total}")
