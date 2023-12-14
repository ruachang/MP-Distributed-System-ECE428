# How to config and run hadoop

## Install Hadoop from binary file

## Hadoop Cluster Setup

(password: su81Excel~ent3)
* start the system
  * `start-dfs.sh`
  * `start-yarn.sh`
* stop the system
  * `stop-dfs.sh`
  * `stop-yarn.sh`

As every night the virtual machine will be restarted, every time logs in the system we need to restart the hdfs system and the file we upload is stored in the cloud. 
To check whether the system is working, using `jps` to see whether the output is
* On machine 1
```
2827195 Jps
2808722 SecondaryNameNode
2808819 NodeManager
2808596 DataNode
```
* On machine 2
```
3968 NodeManager
3318 NameNode
3565 DataNode
5613 Jps
```
* On machine 3
```
2750189 Jps
2731807 NodeManager
2731457 ResourceManager
2731284 DataNode
```
* On other machine
```
2684786 Jps
2666597 DataNode
2666727 NodeManager
```

To restart the system, we need to:
1. Start the `NameNode` on machine 2 using `start-dfs.sh`
2. Start the `ResourceNode` and `NodeManager` on machine 3 using `start-yarn.sh`
Now logging on machine can be finished by `ssh fa23-cs425-800<>`(We don't need the hostname anymore)
## HDFS file system running

After start the HDFS system, just upload and download the file in HDFS system like in the SDFS system

(See from vm2 ~/.bash_history to check some cmd)
Some hdfs command:
* `hdfs dfs -help`
* `hdfs dfs -ls <dir>`: the directory of hdfs system
  * Now the system has `/user/changl25` and `/user/input` dir
* `hdfs dfs -put <local> <sdfs>`
* `hdfs dfs -cat <sdfs>`

Run example
source code of the example: [Github](https://github.com/naver/hadoop/tree/master/hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples)
`cd /home/changl25/hadoop-3.3.6/share/hadoop/mapreduce`, example is in `hadoop jar hadoop-mapreduce-examples-3.3.6.jar` 

* wordcount: `hadoop jar hadoop-mapreduce-examples-3.3.6.jar wordcount <input txt on hdfs> <output dir on hdfs>`