#! /usr/bin/python
import sys
import os
import math

# The program generates regions (MBBs) for fixed-grid 
# partitioning. 
# Input params: space_min_x, space_min_y, space_max_x,
#        space_max_y, bucket_size, total_number_objects
#
# The output is the normalized MBR of regions for partitions
def main():
    if len(sys.argv) != 7:
        sys.exit("Not enough arguments (6)")

    # Dimensions of the universe/space
    min_x = float(sys.argv[1])
    min_y = float(sys.argv[2])
    max_x = float(sys.argv[3])
    max_y = float(sys.argv[4])
    bucket_size = int(sys.argv[5])
    num_object = int(sys.argv[6])

    span_x = max_x - min_x
    span_y = max_y - min_y

    # Determine/Approximate the number of splits along the x and y axis
    x_split = y_split = 1
    if span_y > span_x:
        # We prefer to split into more-square regions than long rectangles
        y_split = math.ceil(math.sqrt(num_object / bucket_size * (span_y /
                                                                  span_x)))
        x_split = math.ceil(num_object / bucket_size / y_split)
    else:
        x_split = math.ceil(math.sqrt(num_object / bucket_size * (span_x /
                                                                  span_y)))
        y_split = math.ceil(num_object / bucket_size / x_split)

    #sys.stderr.write("num_x_split", str(x_split))
    #sys.stderr.write("num_y_split", str(y_split))

    xtile = 1.0 / x_split
    ytile = 1.0 / y_split
    id = 0
    x = 0
    y = 0
    for  x in range(0, x_split) :
        for y in range(0, y_split) :
            id += 1
    #        print("\t".join((str(id), str(min_x + x * xtile ),
    #                       str(min_y + y * ytile),
    #                       str(min_x + (x + 1) * xtile),
    #                        str(min_y + (y + 1) * ytile))))
            print("\t".join((str(id), str( x * xtile ),
                           str(y * ytile),
                           str((x + 1) * xtile),
                            str((y + 1) * ytile)))) 
            y += 1
        x += 1

    sys.stdout.flush()

if __name__ == '__main__':
    main()
