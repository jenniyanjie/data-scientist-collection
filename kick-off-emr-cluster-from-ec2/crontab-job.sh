#!/bin/sh
YYYY=$(date +%Y)
MM=$(date +%m)
DD=$(date +%d)
/usr/bin/python /home/ubuntu/project/aoc-emr-spark/bid-request-aerospike/run_daily.py $YYYY $MM $DD \
    >/tmp/bid-request-aerospike.stdout 2>/tmp/bid-request-aerospike.stderr