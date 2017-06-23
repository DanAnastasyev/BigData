#!/bin/bash
awk -v FS='\t' -v OFS='\t' '{print $2, $1}'
