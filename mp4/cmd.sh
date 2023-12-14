maple map.py 3 task traffic2 $Fiber
juice reduce.py 3 task Traffic_Signal_Intersections_res 1
maple map1.py 1 mid Traffic_Signal_Intersections_res
juice reduce1.py 1 mid result 1
get result /home/changl25/mp4/demo_res/result
select all from traffic2 where None
select all from traffic2 where 9AB971C5-7EFD-4CB3-9B7F-9561976FEAF3
select all from Apartments.csv Apartments2.csv where Stories Stories
put /home/changl25/mp4/Traffic_Signal_Intersections_2.csv traffic2

# filter simple test
put /home/changl25/mp4/Traffic_Signal_Intersections.csv traffic
select all from traffic where None
select all from traffic where 9AB971C5-7EFD-4CB3-9B7F-9561976FEAF3
./hadoop_script/select_hadoop.sh 9AB971C5-7EFD-4CB3-9B7F-9561976FEAF3
./hadoop_script/select_hadoop.sh None 
# join simple test
put /home/changl25/mp4/Apartments.csv traffic2
put /home/changl25/mp4/unique_stories.csv match
select all from traffic2 match where OBJECTID Stories
select all from traffic match where OBJECTID Stories
# easy test small file: /user/changl25/cross_compile/Apartments.csv
./hadoop_script/join_hadoop.sh OBJECTID Stories
# easy test big file: /user/changl25/cross_compile/Traffic_Signal_Intersections.csv 
./hadoop_script/join_hadoop.sh Stories Stories 

# join complex test 
select all from traffic2 match where Stories Stories
select all from traffic match where FACILITYID Stories
# easy test small file: /user/changl25/cross_compile/Apartments.csv

./hadoop_script/join_hadoop.sh Stories Stories
./hadoop_script/join_hadoop.sh Stories Stories
