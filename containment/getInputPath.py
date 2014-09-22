#! /usr/bin/python

import sys
import math

# Generate the command line argument for MapReduce 
#  to perform the containment query
# Command line argument (query window): min_x min_y max_x max_y
#                       
# Standard input: region MBRs
# Output format: -input path_1 -input path_2 ... -input path_n

# Determines whether two rectangles (MBRs) intersect
def intersect(min_x, min_y, max_x, max_y,
              min_x2, min_y2, max_x2, max_y2):
    return not (min_x > max_x2 or max_x < min_x2 or
               min_y > max_y2 or max_y < min_y2)

def main():
    w_min_x = float(sys.argv[1])
    w_min_y = float(sys.argv[2])
    w_max_x = float(sys.argv[3])
    w_max_y = float(sys.argv[4])

    str = ""

    for line in sys.stdin:
        sp = line.strip().split("\t")
        minx = float(sp[1])
#        minx = (minx - space_min_x) / space_x_span
        miny = float(sp[2])
#        miny = (miny - space_min_y) / space_y_span
        maxx = float(sp[3])
#        maxx = (maxx - space_min_x) / space_x_span
        maxy = float(sp[4])
#        maxy = (maxy - space_min_y) / space_y_span
        fileName = sp[5]

        str += " -input "
        str += fileName
        #str.append(" -input ").append(fileName)
    
    sys.stdout.write(str)

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

