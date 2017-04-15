# Big Data Project
## Joyce Wu, Felipe Ducau, Alex Simonoff

###General Running Instructions
These instructions assume you have the NYPD Complaint Data uploaded to HFS. If not, hfs -put NYPD_Complaint_data into your /user/NET_ID folder
To ensure we have clean data, first run:
  * spark-submit clean.py user/YOUR_NETID/NYPD_COMPLAINT_DATA
  * hfs -getmerge output.out output.out
  * hfs -put output.out cleaned_data.csv

For every script in the main bigdataproject folder (unless specified below) the general running instruction is:
  * spark-submit SCRIPT_NAME.py /user/YOUR_NETID/cleaned_data.csv

###Special Cases
agg2cols.py:
  * This script will count instances in the data for two columns as keys
  * run: spark-submit agg2cols.py 13 11 /user/YOUR_NETID/cleaned_data.csv

###Submission Folder
These scripts are all run via the runall.sh bash script

