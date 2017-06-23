#!/usr/bin/env python3

import sys
import re

domain_pattern = re.compile(r'\.\w+(/|\Z)')

def replace(matchobj):
	if matchobj.group(1) == '/':
		return '.com/'
	return '.com'

for line in sys.stdin:
	fields = line.strip().split('\t')
	fields[1] = domain_pattern.sub(replace, fields[1])
	print('\t'.join(fields))