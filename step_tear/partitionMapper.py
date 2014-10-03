#!/usr/bin/env python

from rtree import index
import sys
import os

def main():
    listRegions = []

    idx = index.Index()

    min_x = float(sys.argv[1])
    min_y = float(sys.argv[2])
    max_x = float(sys.argv[3])
    max_y = float(sys.argv[4])
    span_x = max_x - min_x
    span_y = max_y - min_y
    filename = sys.argv[5]
    
    # Open Region MBB file
    partfile = open("filename", "r")
    for line in partfile:
        arr = line.split()
        # For OSM:
        #idx.insert(int(arr[0]), ( float(arr[1]) * 360.0 - 180.0, float(arr[2]) * 180.0 - 90, float(arr[3]) * 360.0 - 180.0, float(arr[4]) * 180.0 - 90.0) )
        
        idx.insert(int(arr[0]), ( float(arr[1]) * span_x + min_x, float(arr[2])*
                                 span_y + min_y, float(arr[3]) * span_x + min_x,
                                 float(arr[4]) * span_y + min_y) )

    for line in sys.stdin:
        arr = line.strip().split("\t")
	    #geom = arr[len(arr) - 1][9:-2].replace("(", "").replace(")", "")
        geom = arr[len(arr) - 1][9:-2].replace("(", "").replace(")", "")

        pairs = geom.split(",")
        xlist = []
        ylist = []
        for p in pairs:
            tmp = p.split(" ")
            xlist.append(float(tmp[0]))
            ylist.append(float(tmp[1]))
        min_x = min(xlist)
        min_y = min(ylist)
        max_x = max(xlist) 
        max_y = max(ylist)

       # [sys.stdout.write("\t".join((str(n), arr[0], arr[len(arr) - 1], "\n" )) ) for n in list(idx.intersection((min_x, min_y, max_x, max_y))) ]
        for n in list(idx.intersection((min_x, min_y, max_x, max_y))):
              sys.stdout.write("\t".join((str(n), arr[0], arr[len(arr) - 
1], "\n" )) )

#        for reg in listRegions:
 #           if not (min_x > reg[3] or min_y > reg[4] or max_x < reg[1] or max_y < reg[2] ) :
  #                  sys.stdout.write("\t".join((reg[0], arr[0], arr[len(arr) - 1], "\n" )) )
              
 
    sys.stdout.flush()

if __name__ == '__main__':
    main()
