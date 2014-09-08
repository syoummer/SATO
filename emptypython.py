#! /usr/bin/python

import sys
import math
import numpy as np

def dataset_size(filename):

def main():
    if len(sys.argv) <2:
        print "Usage: "+ sys.argv[0] + " [partition file]"
        sys.exit(1)

    for line in sys.stdin:
        sp = line.strip().split()
        if (len(sp) == 6):
            dicc[int(sp[0])] +=1

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

