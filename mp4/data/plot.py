import matplotlib.pyplot as plt
import numpy as np
import math
# large file: 80MB, Traffic_Signal_Intersections.csv
# filtered phrase: 9AB971C5-7EFD-4CB3-9B7F-9561976FEAF3 / None 
# join phrase: OBJECTID / Stories(esay) FACILITYID Stories(hard)
# small file: 1MB, Apartments.csv 
# filtered phrase: 9AB971C5-7EFD-4CB3-9B7F-9561976FEAF3 / None 
# join phrase: OBJECTID / Stories(esay) Stories Stories(hard)
# # First group of data
server_num = [5, 10]
# Traffic_Signal_Intersections.csv : 9AB971C5-7EFD-4CB3-9B7F-9561976FEAF3
server_filter_regx_simple_large = np.array([[12, 15, 13], [11, 11, 12]])
server_filter_regx_simple_small = np.array([[0.12, 0.11, 0.1, 0.13], [1.4, 1.5, 1.5, 1.4]])

hadoop_filter_regx_simple_small = np.array([[14, 13, 14], [12, 11, 13]])
hadoop_filter_regx_simple_large = np.array([[28, 25, 27], [26, 25, 24]])
# Traffic_Signal_Intersections: None
server_filter_regx_hard_large = np.array([[9, 7, 8], [6, 7, 4]])
server_filter_regx_hard_small = np.array([[0.3, 0.25, 0.18], [1.4, 1.8, 1.5]])
hadoop_filter_regx_hard_large = np.array([[25, 24, 26], [22, 22, 21]])
hadoop_filter_regx_hard_small = np.array([[6, 7, 4], [5, 6, 3]])

# OBJECTID Stories
file_size = [1, 80]
server_join_regx_simple_all = np.array([[17, 19, 15] , [42, 45, 35]])
hadoop_join_regx_simple_all = np.array([[20, 30], [62, 75]])
# Stories Stories
server_join_regx_hard_all = np.array([[20, 19, 20] , [60, 44, 50]])
hadoop_join_regx_hard_all = np.array([[19, 21], [71, 90]])

server_join_regx_simple_half = np.array([[20, 36, 16] , [62, 85, 120]])
hadoop_join_regx_simple_half = np.array([[30, 31], [70, 90]])
# Stories Stories
server_join_regx_hard_half = np.array([[20, 20, 21] , [70, 90, 85]])
hadoop_join_regx_hard_half = np.array([[24, 22], [80, 90]])
# Create a scatter plot
# plt.figure(figsize=(8, 6))
# plt.subplot(1, 2, 1)
# plt.plot(file_size_1, replicate_time, marker="o")
# plt.errorbar(file_size_1, replicate_time, yerr=[0.025, 0.024, 0.026, 0.027, 0.024], fmt='o', capsize=2)
# # Add labels and a legend
# plt.xlabel("File Size(MB)", fontsize=13)
# plt.ylabel("Replicate Time(second)", fontsize=13)
# plt.title("Renew time after one failure", fontsize=15)
# # plt.legend()

# # plt.figure(figsize=(8, 6))
# plt.subplot(1, 2, 2)
# plt.plot(file_size_1, bandwidth, marker="o")
# plt.errorbar(file_size_1, bandwidth, yerr=[1000, 2000, 1500, 2100, 2300], fmt='o', capsize=4)
# plt.xlabel("File Size(MB)", fontsize=13)
# plt.title("Bandwidth when renewing after one failure", fontsize=15)
# plt.ylabel("Bandwidth(bps)", fontsize=13)
# # Show the plot
# plt.grid(True)

bar_width = 0.2
colors = [(254/255, 67/255, 101/255), (249/255, 205/255, 173/255), (200/255, 200/255, 169/255)]
# Create an array for the x-axis positions
# x = np.arange(len(file_size_2))
# plt.figure(figsize=(8, 6))
# plt.bar(x - 0.2, np.average(insert_time, axis=1), yerr=np.std(insert_time, axis=1), capsize=4, width= 0.2, color=colors[0], label="upload", alpha = 0.5)
# plt.bar(x, np.average(get_time, axis=1), yerr=np.std(get_time, axis=1), capsize=4, width= 0.2, color = colors[1], label = "download")
# plt.bar(x + 0.2, np.average(update_time, axis=1), yerr=np.std(update_time, axis=1), capsize=4, width= 0.2, color=colors[2], label = "update")
# plt.legend()
# plt.xlabel("File Size(MB)", fontsize=13)
# plt.ylabel("Time(second)", fontsize=13)
# plt.title("Time for different commands with different file size", fontsize=15)
# plt.xticks(x, file_size_2, fontsize=12)

x = np.arange(len(server_num))

plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 1)
plt.bar(x - 0.2, np.average(server_filter_regx_simple_small, axis=1),yerr=np.std(server_filter_regx_simple_small, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_filter_regx_simple_small, axis=1),yerr=np.std(hadoop_filter_regx_simple_small, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("Numbers of machines", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to filter(simple)(small file)", fontsize=15)
plt.xticks(x, server_num, fontsize=12)
plt.legend()
# plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 2)
plt.bar(x - 0.2, np.average(server_filter_regx_hard_small, axis=1),yerr=np.std(server_filter_regx_hard_small, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_filter_regx_hard_small, axis=1),yerr=np.std(hadoop_filter_regx_hard_small, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("Numbers of machines", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to filter(hard)(small file)", fontsize=15)
plt.xticks(x, server_num, fontsize=12)
plt.legend()
plt.suptitle('Comparison of Filtering Times between numbers of servers for small file', fontsize=16)
x = np.arange(len(server_num))

plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 1)
plt.bar(x - 0.2, np.average(server_filter_regx_simple_large, axis=1),yerr=np.std(server_filter_regx_simple_large, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_filter_regx_simple_large, axis=1),yerr=np.std(hadoop_filter_regx_simple_large, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("Numbers of machines", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to filter(simple)(large file)", fontsize=15)
plt.xticks(x, server_num, fontsize=12)
plt.legend()
# plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 2)
plt.bar(x - 0.2, np.average(server_filter_regx_hard_large, axis=1),yerr=np.std(server_filter_regx_hard_large, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_filter_regx_hard_large, axis=1),yerr=np.std(hadoop_filter_regx_hard_large, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("Numbers of machines", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to filter(hard)(large file)", fontsize=15)
plt.xticks(x, server_num, fontsize=12)
plt.legend()

plt.suptitle('Comparison of Filtering Times between numbers of servers for large file', fontsize=16)

plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 1)
plt.bar(x - 0.2, np.average(server_join_regx_simple_all, axis=1),yerr=np.std(server_join_regx_simple_all, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_join_regx_simple_all, axis=1),yerr=np.std(hadoop_join_regx_simple_all, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("File size(MB)", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to join(simple)(all machines)", fontsize=15)
plt.xticks(x, file_size, fontsize=12)
plt.legend()
# plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 2)
plt.bar(x - 0.2, np.average(server_join_regx_hard_all, axis=1),yerr=np.std(server_join_regx_hard_all, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_join_regx_hard_all, axis=1),yerr=np.std(hadoop_join_regx_hard_all, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("File size(MB)", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to filter(hard)(all machines)", fontsize=15)
plt.xticks(x, file_size, fontsize=12)
plt.legend()

plt.suptitle('Comparison of Join Times between file size for all machines', fontsize=16)

plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 1)
plt.bar(x - 0.2, np.average(server_join_regx_simple_half, axis=1),yerr=np.std(server_join_regx_simple_half, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_join_regx_simple_half, axis=1),yerr=np.std(hadoop_join_regx_simple_half, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("File size(MB)", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to join(simple)(half join machines)", fontsize=15)
plt.xticks(x, file_size, fontsize=12)
plt.legend()
# plt.figure(figsize=(8, 6))
plt.subplot(1, 2, 2)
plt.bar(x - 0.2, np.average(server_join_regx_hard_half, axis=1),yerr=np.std(server_join_regx_hard_half, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[0], label = "SDFS")
plt.bar(x + 0.2, np.average(hadoop_join_regx_hard_half, axis=1),yerr=np.std(hadoop_join_regx_simple_half, axis=1) , capsize=4, width= 0.4, alpha = 0.6, color = colors[1], label = "HDFS")
plt.xlabel("File size(MB)", fontsize=13)
plt.ylabel("Time(second)", fontsize=13)
plt.title("Time to filter(hard)(half join machines)", fontsize=15)
plt.xticks(x, file_size, fontsize=12)
plt.legend()

plt.suptitle('Comparison of Join Times between file size for half machines', fontsize=16)



plt.show()