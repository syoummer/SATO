 #! /bin/bash

# Extract the mbbs from spatial objects
#INPUT_1=/user/testuser/sample/osm1tsv
# Local data
INPUT_1=../step_sample/osm.1.tsv

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

# Partition data/generate MBBs for the regions
INPUT_4=/user/testuser/sample/osm1mbbnorm
INPUT_4B=/user/testuser/sample/osm2mbbnorm
OUTPUT_4=/user/testuser/sample/mbbfilenorm
OUTPUT_4LOCAL=mbbnormfile
BUCKET_FILE=../step_analyze/partitionSizeFile
#hdfs dfs -cat ${INPUT_4}/* > osmmbbnorm
INPUT_4LOCAL=osmmbbnorm

read bucketsize < ${BUCKET_FILE} 
read numPartitions < $(wc -l ${OUTPUT_4})
echo "Bucket size:"${bucketsize}

# Space Filling Curve - Hilbert curve - partitioning
sfc/serial/hc -b ${bucketsize} -i ${INPUT_4LOCAL} > ${OUTPUT_4LOCAL}

# Binary space partitioning:
#bsp/serial/bsp -b ${bucketsize} -i ${INPUT_4LOCAL} > ${OUTPUT_4LOCAL}

# Fixed grid partitioning
# Misconfigured: ../tiler/hgtiler!

# Remap the original data using generated mbb normalized file
INPUT_5=${INPUT_1}
#OUTPUT_5=/user/testuser/sample/finalpart
OUTPUT_5=osm.1.parted.tsv
MAPPER_5=partitionMapper.py
MBB_NORM_FILE=mbbnormfile

./partitionMapper.py ${min_x} ${min_y} ${max_x} ${max_y} < ${INPUT_5} > ${OUTPUT_5}

#hadoop jar ${HADOOP_STREAMING_PATH}/hadoop-streaming.jar -input ${INPUT_1} -output ${OUTPUT_5} -file ${MAPPER_5} -mapper "partitionMapper.py ${min_x} ${min_y} ${max_x} ${max_y}" -reducer None -numReduceTasks 0

#rm ${BUCKET_FILE}

./updateIndexFile.py ${MBB_NORM_FILE} ${numPartition}
