# MP1
## Description

The MP is to query distributed log files on different machines. We have N(N = 10) machines. If the client runs the program on one machine's terminal, the command `grep` will be executed on all the machines and the result will be printed on the opened terminal.

***Input***

* The pattern you want to query and the options of the `grep` command. (Prove that you have `.log` file on the queried machines)

***Output***

* The number of the connected machines
* The matching result, including the matched lines, the number of these lines
* The time cost on each of machine and the average time cost of finishing query and its standard deviation

## Steps to Use

To get the query result, we need several steps to initially set up all the servers on the ditributed systems. 

1. Run `python3 server.py` on all the machines that want to be queried.
2. Run `python3 client.py <PATTERN> <OPTION>` on terminal of one of the machines.

Then the result of the query will be printed on the terminal.

## Authors and acknowledgment
* [Chang Liu](mailto:changl25@illinois.edu)
* [An Phan](mailto:anphan2@illinois.edu)