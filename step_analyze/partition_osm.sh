 #! /bin/bash

# Extract the mbbs from spatial objects
INPUT_1=/user/hoang/sample/osm1tsv

# Determine the min, max dimensions of the space
INPUT_2=/user/hoang/sample/osm1mbb
OUTPUT_2=/user/hoang/sample/osm1mbbstat
DIMENSION_PATH=getSpaceDimension.py

#hdfs dfs -rm -r ${OUTPUT_2}
#hadoop jar ${HADOOP_STREAMING_PATH}/hadoop-streaming.jar -input ${INPUT_2} -output ${OUTPUT_2} -file ${DIMENSION_PATH} -mapper "getSpaceDimension.py 1" -reducer "getSpaceDimension.py 0" -numReduceTasks 1

## Normalize the space using the dimension obtained from above
TEMP_FILE_NAME=tmpSpaceDimension
#rm $TEMP_FILE_NAME
#hdfs dfs -cat ${OUTPUT_2}/part-00000 > ${TEMP_FILE_NAME}
#SPACE_MIN_X=${cat  ${TEMP_FILE_NAME} | cut -f1 }

#min_x=$(awk '{print $1}' tmpSpaceDimension)
min_x=`(cat ${TEMP_FILE_NAME} | cut -f1)`
min_y=`(cat ${TEMP_FILE_NAME} | cut -f2)`
max_x=`(cat ${TEMP_FILE_NAME} | cut -f3)`
max_y=`(cat ${TEMP_FILE_NAME} | cut -f4)`
num_objects=`(cat ${TEMP_FILE_NAME} | cut -f5)`

# Outputting the space dimensions
echo ${min_x}
echo ${max_x}
echo ${min_y}
echo ${max_y}

# Normalize the mbbs
INPUT_4=/user/hoang/sample/osm1mbbnorm
INPUT_4B=/user/hoang/sample/osm2mbbnorm
OUTPUT_4=/user/hoang/sample/mbbfilenorm




#hdfs dfs -rm -r ${OUTPUT_4}

#hadoop jar ${HADOOP_STREAMING_PATH}/hadoop-streaming.jar -input ${INPUT_3} -output ${OUTPUT_3} -file ${MBB_NORM_PATH} -mapper "mbbnorm.py ${min_x} ${min_y} ${max_x} ${max_y}" -reducer None -numReduceTasks 0

# Determine the optimal bucket count
totalSize=`(hdfs dfs -du -s /user/hoang/sample/osm1tsv | cut -d\  -f1)`
echo "Total size in bytes: "${totalSize}
echo "Number of objects: "${num_objects}
avgObjSize=$((totalSize / num_objects))

blockSize=1600000
partitionSize=$((blockSize / avgObjSize))
echo ${partitionSize} > partitionSizeFile
