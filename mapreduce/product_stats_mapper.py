#!/usr/bin/env python3
"""MapReduce job 2 - mapper: per-product rating statistics.

Output: <parent_asin>\t<rating>\t<helpful_vote>\t<verified 0/1>
"""
import json
import sys

for line in sys.stdin:
    try:
        r = json.loads(line)
        asin, rating = r["parent_asin"], int(r["rating"])
    except (ValueError, KeyError, TypeError):
        sys.stderr.write("reporter:counter:BuyBoxAnalytics,BadRecords,1\n")
        continue
    print(f"{asin}\t{rating}\t{int(r.get('helpful_vote') or 0)}\t{int(bool(r.get('verified_purchase')))}")
