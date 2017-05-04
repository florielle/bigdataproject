## Part II scripts
All data for this part of the project have been uploaded to Dropbox and can be accessed at https://www.dropbox.com/sh/llwzolpa8kwucnb/AABPfc_eipd8qAos78l36gSta?dl=0

### Weather files
`rain.py`, `snow.py` and `temp.py` were all run with `weather.tsv`. They can all be run with the following command:

```
spark-submit SCRIPT_NAME.py weather.tsv
```

### Other Scripts
`avg_day_noise_complaints.py` is to be run on noise complaint filtered 311 Complaint data can be run with the following command:
```
spark-submit avg_day_noise_complaints.py noise_complaints.csv
```
