#! /bin/bash

# Configuring lib and include directories
usage(){
  echo -e "containmentQuery.sh [options]\n \
  -p HDFS_PATH_PREFIX, --prefix=HDFS_PATH_PREFIX \t directory path to the include locations \n \
  -d DESTINATION_RESULT_PATH, --destination=DESTINATION_RESULT_PATH \t The destination for the result data (HDFS path) \n \
  -i MIN_X, --min_x=MIN_X \t The smallest x-coordinate of the space \n \
  -j MIN_Y, --min_y=MIN_Y \t The smallest y-coordinate of the space \n \
  -k MAX_X, --max_x=MAX_X \t The largest x-coordinate of the space \n \
  -l MAX_Y, --max_y=MAX_Y \t The largest y-coordinate of the space \n \
"
 # -i OBJECT_ID, --obj_id=OBJECT_ID \t The field (position) of the object ID \n \
  exit 1
}

# Default empty values
datapath=""
destination=""


while : 
do
    case $1 in
        -h | --help | -\?)
          usage;
          exit 0
          ;;
        -d | --destination)
          destination=$2
          shift 2
          ;;
        --destination=*)
          destination=${1#*=}
          shift
          ;;
        -p | --prefix)
          datapath=$2
          shift 2
          ;;
        --prefix=*)
          datapath=${1#*=}
          shift
          ;;
        -i | --min_x)
          min_x=$2
          shift 2
	  ;;
        --min_x=*)
          min_x=${1#*=}
          shift
          ;;
        -j | --min_y)
          min_y=$2
          shift 2
	  ;;
        --min_y=*)
          min_y=${1#*=}
          shift
          ;;
        -k | --max_x)
          max_x=$2
          shift 2
	  ;;
        --max_x=*)
          max_x=${1#*=}
          shift
          ;;
        -l | --max_y)
          max_y=$2
          shift 2
	  ;;
        --max_y=*)
          max_y=${1#*=}
          shift
          ;;
        --)
          shift
          break
          ;;
        -*)
          echo "Unknown option: $1" >&2
          shift
          ;;
        *) # Done
          break
          ;;
     esac
done

# Setting global variables
HJAR=${HADOOP_STREAMING_PATH}/hadoop-streaming.jar

# Load the SATO configuration file
source ../sato.cfg
LD_CONFIG_PATH=${LD_LIBRARY_PATH}:${SATO_LIB_PATH}

PARTITION_FILE=partfile

# The location of the index file
index_location=$5

PATH_RETRIEVER=../containment/getInputPath.py

if [ ! "${min_x}" ] && [! "${min_y}" ] && [! "${max_x}" ] && [! "${max_y}" ]; then
     echo "ERROR: Missing query window dimensions"
     exit 1
fi

if [ ! "${datapath}" ]; then
     echo "Error: Missing path to the loaded data"
     exit 1
fi

if [ ! "${destination}" ]; then
     echo "Error: Missing path for the result/destination"
     exit 1
fi

# We can use mktemp as well
rm -f ${PARTITION_FILE}
hdfs dfs -get ${datapath}/${PARTITION_FILE} ./
input_path=`( ../containment/getInputPath.py ${min_x} ${min_y} ${max_x} ${max_y} ${datapath}/data/ < "${PARTITION_FILE}" )`

#../containment/getInputPath.py ${min_x} ${min_y} ${max_x} ${max_y} ${datapath} < "${PARTITION_FILE}"

# Load the configuration file to determine the geometry field
DATA_CFG_FILE=data.cfg
rm -f ${DATA_CFG_FILE}
hdfs dfs -get ${datapath}/${DATA_CFG_FILE} ./
source ${DATA_CFG_FILE}

echo $input_path

MAPPER_1=containment
MAPPER_1_PATH=../joiner/containment
OUTPUT_1=${destination}
hdfs dfs -rm -f -r ${destination}

echo "Querying: $min_x $min_y $max_x $max_y"
echo ${input_path}
hadoop jar ${HJAR} ${input_path} -output ${destination} -file ${MAPPER_1_PATH} -mapper "${MAPPER_1} ${min_x} ${min_y} ${max_x} ${max_y} ${geomid}" -reducer None --cmdenv LD_LIBRARY_PATH=${LD_CONFIG_PATH} -numReduceTasks 0
#echo ${input_path}
