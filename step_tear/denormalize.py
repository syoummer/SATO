#!/usr/bin/env python

import sys

def main():
    if len(sys.argv) != 5:
	print("Usage: " + sys.argv[0] + " [min_x] [min_y] [max_x] [max_y]")
	sys.exit(1)

    glob_min_x = float(sys.argv[1])
    glob_min_y = float(sys.argv[2])
    glob_max_x = float(sys.argv[3])
    glob_max_y = float(sys.argv[4])
    span_x = glob_max_x - glob_min_x
    span_y = glob_max_y - glob_min_y

    for line in sys.stdin:
        arr = line.strip().split("\t")
 	min_x = float(arr[1])
	min_y = float(arr[2])
	max_x = float(arr[3])
	max_y = float(arr[4])
        min_x = min_x * span_x + glob_min_x
        max_x = max_x * span_x + glob_min_x
        min_y = min_y * span_y + glob_min_y
        max_y = max_y * span_y + glob_min_y

        print("\t".join((arr[0], str(min_x), str(min_y), str(max_x), str(max_y))))
 
    sys.stdout.flush()

if __name__ == '__main__':
    main()
