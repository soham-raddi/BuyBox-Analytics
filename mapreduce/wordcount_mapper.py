#!/usr/bin/env python3
"""MapReduce job 1 - mapper: complaint word count.

Input : raw review JSON Lines (semi-structured, one JSON object per line)
Output: <word>\t1 for every non-stopword in reviews with rating <= 2
"""
import json
import re
import sys

STOP = set("""a about above after again against all am an and any are as at be because been before being below
between both but by can did do does doing down during each few for from further had has have having he her
here hers herself him himself his how i if in into is it its itself just me more most my myself no nor not now
of off on once only or other our ours ourselves out over own same she should so some such than that the their
theirs them themselves then there these they this those through to too under until up very was we were what
when where which while who whom why will with you your yours yourself yourselves it's i'm don't didn't doesn't
can't product one would get got also use used using even really like bought item amazon buy could much well
make made way first two""".split())
TOKEN = re.compile(r"[a-z']{3,}")

for line in sys.stdin:
    try:
        review = json.loads(line)
    except ValueError:
        sys.stderr.write("reporter:counter:BuyBoxAnalytics,MalformedJSON,1\n")   # Hadoop counter
        continue
    if review.get("rating") is None or review["rating"] > 2:
        continue
    sys.stderr.write("reporter:counter:BuyBoxAnalytics,NegativeReviews,1\n")
    for word in TOKEN.findall((review.get("text") or "").lower()):
        if word not in STOP:
            print(f"{word}\t1")
