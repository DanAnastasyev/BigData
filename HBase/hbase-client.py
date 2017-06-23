#!/usr/bin/env python3

import argparse
import calendar
import getpass
import happybase
import logging
import random
import sys
from collections import Counter

logging.basicConfig(level="DEBUG")

HOSTS = ["mipt-node0%d.atp-fivt.org" % i for i in range(1, 5)]

USAGE = """
Format of query:
  $ {0} --table_name (e.g. s201723Table) --period_from (e.g. 2014-09-01) --period_to (e.g. 2014-09-10)
""".format(sys.argv[0])

def connect(table_name):
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)

    logging.debug("Connecting to HBase Thrift Server on %s", host)
    conn.open()

    logging.debug("Using table %s", table_name)
    return happybase.Table(table_name, conn)

def parse_date(date):
    year, month, day = date.split('-')
    return int(year), int(month), int(day)

def get_period(period_from, period_to):
    year_from, month_from, day_from = parse_date(period_from)
    year_to, month_to, day_to = parse_date(period_to)
    def get_row_name(year, month):
        return "%04d%02d" % (year, month)

    if year_from != year_to or month_from != month_to:
        start_days = ['col_day%02d' % i for i in range(day_from, 32)]
        end_days = ['col_day%02d' % i for i in range(1, day_to)]

        first_month_range = (get_row_name(year_from, month_from), get_row_name(year_from, month_from + 1), start_days)
        last_month_range = (get_row_name(year_to, month_to), get_row_name(year_to, month_to + 1), end_days)
        inner_range = (get_row_name(year_from, month_from + 1), get_row_name(year_to, month_to))
        return first_month_range, inner_range, last_month_range
    else:
        inner_range = (get_row_name(year_from, month_from), 
            get_row_name(year_to, month_to + 1), 
            ['col_day%02d' % i for i in range(day_from, day_to + 1)])
        return None, inner_range, None

def main():
    parser = argparse.ArgumentParser(usage=USAGE)
    parser.add_argument("--table_name", type=str, required=True)
    parser.add_argument("--period_from", type=str, required=True)
    parser.add_argument("--period_to", type=str, required=True)
    args = parser.parse_args()

    table = connect(args.table_name)

    first_month_range, inner_range, last_month_range = get_period(args.period_from, args.period_to)

    def convert_data(data):
        return Counter({col.decode("utf-8")[10:] : int.from_bytes(data[col], byteorder='big') for col in data})

    domain_hits = Counter()
    if first_month_range:
        for key, data in table.scan(row_start=first_month_range[0], row_stop=first_month_range[1], columns=first_month_range[2]):
            domain_hits += convert_data(data)
    for key, data in table.scan(row_start=inner_range[0], row_stop=inner_range[1]):
        domain_hits += convert_data(data)
    if last_month_range:
        for key, data in table.scan(row_start=last_month_range[0], row_stop=last_month_range[1], columns=last_month_range[2]):
            domain_hits += convert_data(data)

    counts_sum = sum(domain_hits.values())
    for pair in domain_hits.most_common():
        print( pair[0], pair[1] / counts_sum, sep='\t' )

if __name__ == "__main__":
    main()
