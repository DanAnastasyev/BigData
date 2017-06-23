#!/usr/bin/env python3

import sys
import os

def read_input(file, separator):
    for line in file:
        yield line.strip().split(separator)

def main(separator='\t'):
    A = os.environ.get('A', 100)
    B = os.environ.get('B', 20)

    cur_url, cur_id = None, None
    cur_unique_id_count, cur_visits_count = 0, 0
    for (url, user_id, count) in read_input(sys.stdin, separator):
        if url == cur_url:
            if user_id != cur_id:
                cur_unique_id_count += 1
                cur_id = user_id
            cur_visits_count += 1
        else:
            if cur_url:
                print('{}\t{}'.format((cur_visits_count + A) / (cur_unique_id_count + B), cur_url))
            cur_url = url
            cur_id = user_id
            cur_unique_id_count = 1
            cur_visits_count = 1
    
    print('{}\t{}'.format((cur_visits_count + A) / (cur_unique_id_count + B), cur_url))


if __name__ == "__main__":
    main()