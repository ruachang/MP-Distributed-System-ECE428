X="$1"
hdfs dfs -rm -r /user/changl25/demo/test1_output
hadoop jar /home/changl25/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-files /home/changl25/mp4/hadoop_script/task_map.py,/home/changl25/mp4/hadoop_script/task_reduce.py \
-mapper "python3 task_map.py $X" \
-reducer "python3 task_reduce.py" \
-input /user/changl25/demo/Traffic_Signal_Intersections_3.csv \
-output /user/changl25/demo/test1_output
