# mp4

## Description

The MP is a Hadoop like distributed file system that can achieve MapleJuice(MapReduce like) tasks efficiently by partitioning and processing the task on several machines seperately.

Any task can be launched on any machine and can be assigned to any machine in the system. When a machine is failed, its task can be resent to other machines so the tasks can always be finished successfully.

## Commands

* `maple map_exe num_maples intermidiate sdfsname $param`
  * Do the maple task, which is predefined in `map/map_exe.py`
  * The result of the maple task is a sequence of files named as intermediate_number and its content is the processed key-value line
* `juice reduce_exe num_reduces intermediate sdfsname delete`
  * Do the reduce task on the intermediate files
  * The result of the reduce task is the final result of the MapleJuice task
* `select all from sdfsfilename where regex`
  * Specify any regular expression for every line in the file
* `select all from sdfsfilename1 sdfsfilename2 where key1 key2`
  * Specify any line that `sdfsfilename1's key1 == sdfsfilename2's key2` and join them together
* `put localfilename sdfsfilename`
  * Upload the local file to the server
* `get sdfsfilename localfilename`
  * Download the server's file to local
* `delete sdfsfilename`
  * delete the file on the whole server
* `ls sdfsfilename`
  * list all the machines that have that file
* `store`
  * list all the file's name this machine stores
* `Multiread vm0,vm1... sdfsfilename localfilename`
  * Downloading simultaneously from the list machine to the local file
* `list_mem`
  * list all the member now in the group
* `print dic`
  * print file to server dictionary in the file system
* `list_self`: print out host ID
* `join` and `leave`: After typing leave, the virtual machine will temporarily leave from the group(stop sending the message to other machines). Then type join to rejoin in the group(restart sending the message to other machines)
* `enable_gossip` and `disable_gossip`: change to the GOSSIP+S mode and change back to GOSSIP mode
* `list_suspected`: display a suspected node immediately at the VM terminal of the suspecting node

## Steps to use sdfs

* Use `python3 server.py` to start the coordinator of group on `machine2`
* Use `python3 server.py` to start machine that wants to join in the group
* `put sdfsfilename localfilename` to upload file to the system. 
* To run the maple and juice task, all of the execution file is in the `map\` and `reduce\`. Replace the `exe` in the command with `<map_exe>.py` to run. If you want to run the demo task, you can directly run `exe.py` to input all of the command to the sdfs.


## Steps to use hadoop 

### Start the hadoop

To restart the system, we need to:
1. Start the `NameNode` on machine 2 using `start-dfs.sh`
2. Start the `ResourceNode` and `NodeManager` on machine 3 using `start-yarn.sh`

To check whether the system is running correctly, we can use `jps` to see whether the output on each machine is right. For example, the output node should be like

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

### run the MapReduce

#### run example MapReduce code

To run program on the hadoop system, we need to upload our files to the hdfs system. The command can be checked from `hdfs dfs -help`.
Some hdfs command:
* `hdfs dfs -ls <dir>`: the directory of hdfs system
* `hdfs dfs -put <local> <sdfs>`
* `hdfs dfs -cat <sdfs>`

Now the hadoop system can run the example code of it and the sql commands achieve by ourselves. 

To run the example, like `wordCount`
1. Go into the target directory by `cd /home/changl25/hadoop-3.3.6/share/hadoop/mapreduce`
2. run wordCount by `hadoop jar hadoop-mapreduce-examples-3.3.6.jar wordcount <input txt on hdfs> <hdfs/output/dir>`

#### run sql command

To run the sql commands achieved also in our dfs, directly use the script in the `hadoop_script` and it will call the map and reduce task written by us. 

`select` needs to upload one file in the hdfs file system. To run the `select`
1. Upload the file to the hdfs system by `hdfs dfs -put /local/path/to/file /user/changl25/cross_compile/file`
2. Run the script by `./home/changl25/mp4/hadoop_script/select_hadoop.sh <input path> <pattern>` and the output will be in the directory `/user/changl25/cross_compile/select_output` in the hdfs system 

`join` needs to upload two datasets' csv file in hdfs file system. To run the `join`
1. Upload the two datasets to the hdfs system by `hdfs dfs -put /local/path/to/file /user/changl25/cross_compile/file`
2. Run the script by `./home/changl25/mp4/hadoop_script/join_hadoop.sh <input dataset1> <key1> <input dataset2> <key2>`, the merged csv will be generated on the hdfs system `/user/changl25/cross_compile/output`

## Authors and acknowledgment
* [Chang Liu](mailto:changl25@illinois.edu)
* [An Phan](mailto:anphan2@illinois.edu)