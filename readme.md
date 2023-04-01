SortByCountryTimestamp

This PySpark script reads an input CSV file, sorts the data by country(cca2) and timestamp(timestamp), and then writes the sorted output to a CSV file.

Requirements
1. PySpark
2. CSV file containing data with columns cca2 (country code) and timestamp (in the format "yyyy-MM-dd HH:mm:ss")

How to run
spark-submit --master local[*] countrytimestamp.py export.csv output.csv
where export.csv is the input file and output.csv is the output file
Note: Keep countrytimestamp.py, export.csv, and run.sh in the same file structure or provide the appropriate path in the command