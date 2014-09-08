#! /usr/bin/python

import sys
import math
import random
from decimal import Decimal

def main():
    if len(sys.argv) < 2:
        print ("Usage: " + sys.argv[0] + "[original delimiter] [percentage to keep]")
        sys.exit(1)

    for line in sys.stdin:
        # Split based on the original field delimiter
        sp = line.strip().split(sys.argv[1])

        # Add your filters here
        # Example:
        if len(sp[0]) < 0: # Skip object if the default id field is less than 0
            continue

        # Uniformly sample
        if random.random() < Decimal(sys.argv[2]):
            print ("\t".join(sp))
            

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

