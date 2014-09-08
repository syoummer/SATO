Python scripts used in the sampling step

1) samplefilter.py arg1 arg2: used to convert the original file where fields are delimited by arg1 into tab-separated format with sampling ratio arg2

  e.g.   Reformat OSM data (fields delimited by | using a sampling ratio of 0.5)
    (local execution)
    >>>   ./samplefilter.py '|' 0.5 < osm.1.raw > osm.1.tsv
    
    (cluster execution)     
    >>>   hadoop ${HADOOP_STREAMING_PATH}/hadoop-streaming.jar -input /user/hoang/sample/osm1raw -output /user/hoang/sample/osm1tsv -file samplefilter.py -mapper "samplefilter.py | 0.5" -reducer None -numReduceTasks 0


