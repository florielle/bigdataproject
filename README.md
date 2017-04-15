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
The `colX.py` files in the submission folder are used to generate a new table which indicates, for every row of the X column of the original dataset the base type (i.e., INT/LONG, DECIMAL, TEXT, maybe DATETIME), a semantic data type (e.g., phone, address, city, state, zipcode) and a label from the set [NULL -> missing or unknown information, VALID -> valid value from the
intended domain of the column, INVALID/OUTLIER -> suspicious or invalid values]. 

Each script can be individualy run with 

```
spark-submit [colX.py] /user/YOUR_NETID/cleaned_data.csv
```

The output will be saved in `colX.out`.

After we ran the column scripts we use the map reduce tasks in the map_reduce folder to count the number of VALID, INVALID and NULL occurrences per column. This is done with the command

```
/usr/bin/hadoop jar /opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hadoop-mapreduce/hadoop-streaming.jar -files "map_reduce/" -mapper "map_reduce/map.py" -reducer "map_reduce/reduce.py" -input [colX.out] -output [col_counts_X.out] 
```
and the output will be saved in `col_counts_X.out`.

The entire procedure of running all the `colX.py` scripts and creating the counts for each column can be ran directly using the runall.sh bash script as follows

```
sh runall.sh
```

which creates all the `colX.out` and `col_counts_X.out` files. 
