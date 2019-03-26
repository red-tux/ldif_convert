#!/bin/bash

DATA_OUT="convert_ldif_stats_$(date +%m%d_%H%M)"

time python -m cProfile -o $DATA_OUT convert_ldif.py