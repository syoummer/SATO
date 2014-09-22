 #! /bin/bash

if [ ! $# -eq 5 ]; then
   echo -e "Not enough argument\n"
   exit -1
fi

# Required Command line arguments - Window query dimensions
min_x=$1
min_y=$2
max_x=$3
max_y=$4
# The location of the index file
index_location=$5

read input_path < <(./getInputPath.py ${min_x} ${min_y} ${max_x} ${max_y} ${index_location})

TEMP_FILE_NAME=../step_analyze/tmpSpaceDimension
min_x=`(cat ${TEMP_FILE_NAME} | cut -f1)`
min_y=`(cat ${TEMP_FILE_NAME} | cut -f2)`
max_x=`(cat ${TEMP_FILE_NAME} | cut -f3)`
max_y=`(cat ${TEMP_FILE_NAME} | cut -f4)`

# Outputting the space dimensions
#echo ${min_x}
#echo ${max_x}
#echo ${min_y}
#echo ${max_y}

OUTPUT_5=/user/testuser/query
MAPPER_QUERY=mapperquery.py

hadoop jar ${HADOOP_STREAMING_PATH}/hadoop-streaming.jar -input ${input_path} -file ${MAPPER_QUERY} -mapper "mapperquery.py ${min_x} ${min_y} ${max_x} ${max_y}" -reducer None -numReduceTasks 0


