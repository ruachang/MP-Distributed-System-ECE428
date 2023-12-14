# MP3

## Description 

The MP is a scalable distributed file system(SDFS) that designed to store and share the file among the systems. The machines can form a group and it will include a leader server to manage all of the files in the system. 

Any machine in system can put and get file to or from the group servers and they can check for the existence of certain file in the system. The system is robust and can tolerate to up to three simultaneous machine failures with minimal replication of files.

## Commands

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

## Steps to use
* Use `python3 server.py` to start the coordinator of group on `machine2`
* Use `python3 server.py` to start machine that wants to join in the group
* `put sdfsfilename localfilename` to upload file to the system. All the commands can be finished by typing in the showing format
  
## Authors and acknowledgment
* [Chang Liu](mailto:changl25@illinois.edu)
* [An Phan](mailto:anphan2@illinois.edu)