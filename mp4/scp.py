import subprocess
# Define the source file and the common destination directory
source_file = '/home/changl25/mp4/README.md'
DFS_PATH = '/home/changl25/mp4/dfs'
user = "changl25"
# List of remote hosts
HOST_NAME_LIST = [
    'fa23-cs425-8001.cs.illinois.edu',
    'fa23-cs425-8002.cs.illinois.edu',
    'fa23-cs425-8003.cs.illinois.edu',
    'fa23-cs425-8004.cs.illinois.edu',
    'fa23-cs425-8005.cs.illinois.edu',
    'fa23-cs425-8006.cs.illinois.edu',
    'fa23-cs425-8007.cs.illinois.edu',
    'fa23-cs425-8008.cs.illinois.edu',
    'fa23-cs425-8009.cs.illinois.edu',
    'fa23-cs425-8010.cs.illinois.edu'
]
# Loop through the list of remote hosts and copy the file to each host
for remote_host in HOST_NAME_LIST:
    # Construct the destination path on the remote host
    destination_path = f'{DFS_PATH}'
    # Construct the SCP command
    scp_command = ['scp', source_file, f'{user}@{remote_host}:{destination_path}']
    try:
        # Execute the SCP command
        subprocess.run(scp_command, check=True)
        print(f'Successfully copied {source_file} to {destination_path}')
    except subprocess.CalledProcessError as e:
        print(f'Error while copying to {remote_host}: {e}')
    except Exception as e:
        print(f'An unexpected error occurred while copying to {remote_host}: {e}')