import random
import string
import datetime
import argparse
import os
# Function to generate random log entries
def generate_log_entry(vm_num):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # if vm_num == 1:
    #     log_level_list = ['MORE_FREQUENT', 'VM_ONE', 'RARE']
    #     prob_list = [0.8, 0.15, 0.05]
    # elif vm_num % 2 == 0:
    #     log_level_list = ['MORE_FREQUENT', 'EVEN_FREQUENT', 'RARE']
    #     prob_list = [0.5, 0.3, 0.2]
    # else:
    #     log_level_list = ['MORE_FREQUENT', 'ODD_FREQUENT','RARE']
    #     prob_list = [0.5, 0.3, 0.2]
    log_level_list = ['MORE_FREQUENT', 'SOMEHOW_FREQUENT','RARE']
    prob_list = [0.8, 0.15, 0.05]
    log_level = random.choices(log_level_list, weights=prob_list)
    message = ''.join(random.choices(string.ascii_letters + string.digits, k=100))
    return f"{timestamp} {log_level} {message}\n"

parser = argparse.ArgumentParser(description='ECE 428')
parser.add_argument('--log_file_path', dest='log_file_path', type=str, 
                    default = os.path.join(os.path.abspath('./'), 'test/log_file'),
                    help='the directory of the training data')
parser.add_argument('--vm_num', dest='vm_num', type=int, default = '10',
                        help='the number of the virtual machine')
parser.add_argument('--lines', dest='lines', type=int, default = '300000',
                        help='the directory of the training data')
args = parser.parse_args()

# Define the log file path
log_file_path = args.log_file_path
vm_num = args.vm_num
desired_lines = args.lines

# Create the log file
for i in range(vm_num):
    file_path = os.path.join(log_file_path, "vm{}.log".format(i + 1))
    with open(file_path, 'w') as log_file:
        for _ in range(desired_lines):
            log_entry = generate_log_entry(i + 1)
            log_file.write(log_entry)

print(f"Log file '{log_file_path}' created with approximately {desired_lines} lines.")
