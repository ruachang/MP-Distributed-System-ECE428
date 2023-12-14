# # * select
pattern="$1"
hdfs dfs -rm -r /user/changl25/demo/select_output*
hadoop jar /home/changl25/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-files /home/changl25/mp4/hadoop_script/select_map.py,/home/changl25/mp4/hadoop_script/select_reduce.py \
-mapper "python3 select_map.py $pattern" \
-reducer "python3 select_reduce.py" \
-input /user/changl25/demo/Traffic_Signal_Intersections.csv \
-output /user/changl25/demo/select_output1
# -input  /user/changl25/cross_compile/Traffic_Signal_Intersections_2.csv \
