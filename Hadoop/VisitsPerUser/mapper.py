#!/usr/bin/env python3

import sys

def read_input(file, separator):
    for line in file:
        yield line.strip().split(separator)

def main(separator='\t'):
    for fields in read_input(sys.stdin, separator):
        user_id, url = fields[0], fields[2]
        print('{}\t{}\t{}'.format(url, user_id, 1))

if __name__ == "__main__":
    main()