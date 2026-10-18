#!/usr/bin/env python3
"""MapReduce job 2 - reducer: for each product emit
<parent_asin> n_reviews avg_rating pct_negative helpful_votes pct_verified  r1 r2 r3 r4 r5 (histogram)
"""
import sys


def emit(asin, hist, helpful, verified):
    n = sum(hist)
    avg = sum((i + 1) * c for i, c in enumerate(hist)) / n
    neg = (hist[0] + hist[1]) / n
    print("\t".join([asin, str(n), f"{avg:.3f}", f"{neg:.3f}", str(helpful), f"{verified / n:.3f}",
                     *map(str, hist)]))


current, hist, helpful, verified = None, [0] * 5, 0, 0
for line in sys.stdin:
    asin, rating, votes, ver = line.rstrip("\n").split("\t")
    if asin != current:
        if current is not None:
            emit(current, hist, helpful, verified)
        current, hist, helpful, verified = asin, [0] * 5, 0, 0
    hist[min(max(int(rating), 1), 5) - 1] += 1
    helpful += int(votes)
    verified += int(ver)
if current is not None:
    emit(current, hist, helpful, verified)
