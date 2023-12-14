import sys 
import subprocess

# col name:pattern
pattern = sys.argv[1]

for line in sys.stdin:
    line = line.rstrip()
    try:
        grep_cmd = f"grep {pattern} {line}"
        result = subprocess.getoutput(grep_cmd)
        
        # If the line matches, grep will return it; otherwise, it will raise CalledProcessError
        print(result)
    except subprocess.CalledProcessError:
        # This error means the line did not match the pattern; do nothing
        pass
