 #! /bin/bash

INPUT_1=/user/hoang/sample/osm1raw
INPUT_2=/user/hoang/sample/osm2raw
OUTPUT_1=/user/hoang/sample/osm1tsv
OUTPUT_2=/user/hoang/sample/osm2tsv

hdfs dfs -rm -r ${OUTPUT_1}

hadoop jar ${HADOOP_STREAMING_PATH}/hadoop-streaming.jar -input ${INPUT_1} -output ${OUTPUT_1} -file samplefilter.py -mapper "samplefilter.py | 0.5" -reducer None -numReduceTasks 0

