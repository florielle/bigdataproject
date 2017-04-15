# Big Data Project
## Joyce Wu, Felipe Ducau, Alex Simonoff

### Data Used
The dataset used for this project can be downloaded from the [NYC Open Data](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i) portal.

### General Running Instructions
These instructions assume you have the NYPD Complaint Data uploaded to HFS. If not it can be done with
```
hfs -put NYPD_Complaint_data into your /user/NET_ID/folder/
```

To ensure we have clean data, first run:
```
spark-submit clean.py user/YOUR_NETID/NYPD_COMPLAINT_DATA
hfs -getmerge output.out output.out
hfs -put output.out cleaned_data.csv
```

For every script in the main bigdataproject folder (unless specified below) the general running instruction is:
```
spark-submit SCRIPT_NAME.py /user/YOUR_NETID/cleaned_data.csv
```

#### Special Cases
##### agg2cols.py
This script will do a groupby for two columns and then count the number of instances per group. 
For example if we want to count the number of felonies, misdemeanors and violations (column 11) for each borough (column 13) we do:
```
spark-submit agg2cols.py /user/YOUR_NETID/cleaned_data.csv 13 11 
```

In general, where [column_1] = # of first column and [column_2] = # of second column, we do:
```
spark-submit agg2cols.py /user/YOUR_NETID/cleaned_data.csv [column_1] [column_2] 
```

##### countuniques.py
This script will count up the number of instances for each unique value in a column specified, where [column] = # of column.
```
spark-submit countuniques.py /user/YOUR_NETID/cleaned_data.csv [column]
```

##### delay_dist.py
This script produce the distribution of delay in occurrence date and report date for a specific offense type. For example, for the crime 'RAPE', we would run:
```
spark-submit countuniques.py /user/YOUR_NETID/cleaned_data.csv "RAPE"
```

### Submission Folder
The .py files for mapping the values to VALID, INVALID, or NULL.
These scripts are all run via the runall.sh bash script

