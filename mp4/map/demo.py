import csv
from collections import Counter
import sys 
# The input parameter 'Interconne' type X.
# Replace this with the value provided by the TA during the demo.
interconne_type = sys.argv[1]  # Example value; 

# File path to the CSV file
file_path = '/home/changl25/mp4/Traffic_Signal_Intersections_2.csv'

# Initialize a counter to keep track of 'Detection_' occurrences
detection_counter = Counter()

# Total count of intersections with 'Interconne' type X
total_intersections = 0

# Read the CSV file
with open(file_path, 'r') as csvfile:
    csv_reader = csv.reader(csvfile)
    headers = next(csv_reader)  # Assume the first row is the header
    
    # Find the index for 'Interconne' and 'Detection_' columns
    interconne_idx = headers.index('Interconne')
    detection_idx = headers.index('Detection_')

    # Iterate over the rows in the CSV file
    for row in csv_reader:
        interconne_value = row[interconne_idx]
        detection_value = row[detection_idx]
        
        # Check if 'Interconne' is of type X
        if interconne_value == interconne_type:
            total_intersections += 1
            detection_counter[detection_value] += 1
print(detection_counter)
# Calculate the percent composition of 'Detection_'
percent_composition = {key: (value / total_intersections) * 100 for key, value in detection_counter.items()}

print(percent_composition)
