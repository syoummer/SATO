#!/usr/bin/env python

import sys
import math


# Extract the bounding rectangles of the entire space
# The input should be tab-separated mbbs generated from step_sample
#   object_id  min_x min_y max_x max_y
# Note: the mbbs are not normalized

def main():
    stat = {}
    stat2 = {}    

    geom1 = -1
    geom2 = -1
    
    for line in sys.stdin:
        sp = line.split("=")
 
        if sp[0] == 'geomid' and geom1 == -1:
             geom1 = int(sp[1])

        elif sp[0] == 'geomid' and geom1 != -1:
             geom2 = int(sp[1])
          
        elif not sp[0] in stat:
             stat[ sp[0] ] = float(sp[1])
        else:
             stat2[ sp[0] ] = float(sp[1])
    

    dataminx = min(stat['dataminx'], stat2['dataminx'])
    dataminy = min(stat['dataminy'], stat2['dataminy'])
    datamaxx = max(stat['datamaxx'], stat2['datamaxx'])
    datamaxy = max(stat['datamaxy'], stat2['datamaxy'])
    numobjects = stat['numobjects'] + stat2['numobjects']
    
    print("dataminx=" + str(dataminx))
    print("dataminy=" + str(dataminy))
    print("datamaxx=" + str(datamaxx))
    print("datamaxy=" + str(datamaxy))
    print("numobjects=" + str(int(numobjects)))
    print("geomid1=" + str(geom1))
    print("geomid2=" + str(geom2))
    
    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

