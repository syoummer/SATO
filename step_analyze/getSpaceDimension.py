#!/usr/bin/env python

import sys
import math


# Extract the bounding rectangles of the entire space
# The input should be tab-separated mbbs generated from step_sample
#   object_id  min_x min_y max_x max_y
# Note: the mbbs are not normalized

def main():
    count = 0
    offset = int(sys.argv[1])
    try:
        line1 = sys.stdin.readline()
        
        sp = line1.strip().split("\t")
            
        minx = float(sp[offset])
        miny = float(sp[offset + 1])
        maxx = float(sp[offset + 2])
        maxy = float(sp[offset + 3])
        if (offset == 0):
            count = int(sp[offset + 4])
        else:
            count = 1

        for line in sys.stdin:
            sp = line.strip().split("\t")
            minx = min(minx, float(sp[offset]))
            miny = min(miny, float(sp[offset + 1]))
            maxx = max(maxx, float(sp[offset + 2]))
            maxy = max(maxy, float(sp[offset + 3]))
            if (offset == 0):
                count = int(sp[offset + 4]) + count
            else:
                count = count + 1

        if count > 0:
            print ("\t".join((str(minx), str(miny), str(maxx), str(maxy),
                              str(count))))

    except (IOError, IndexError):
        pass

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

