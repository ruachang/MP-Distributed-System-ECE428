# * first mapper of join
key1="$1"
key2="$2"
hdfs dfs -rm -r /user/changl25/cross_compile/output*
hadoop jar /home/changl25/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-files /home/changl25/mp4/hadoop_script/join_map.py \
-mapper "python3 join_map.py D1 $key1" \
-input /user/changl25/cross_compile/Traffic_Signal_Intersections.csv \
-output /user/changl25/cross_compile/output1
# /user/changl25/cross_compile/Traffic_Signal_Intersections.csv
# /user/changl25/cross_compile/Apartments.csv
hadoop jar /home/changl25/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-files /home/changl25/mp4/hadoop_script/join_map.py \
-mapper "python3 join_map.py D2 $key2" \
-input /user/changl25/cross_compile/unique_stories.csv \
-output /user/changl25/cross_compile/output2

# # * reduce of join
hadoop jar /home/changl25/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-files /home/changl25/mp4/hadoop_script/identity.py,/home/changl25/mp4/hadoop_script/join_reduce.py \
-mapper "python3 identity.py" \
-reducer "python3 join_reduce.py D1 D2" \
-input /user/changl25/cross_compile/output1,/user/changl25/cross_compile/output2 \
-output /user/changl25/cross_compile/outputt
