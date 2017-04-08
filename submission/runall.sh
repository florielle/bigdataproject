echo "Removing output files from hfs"
for I in {0..22}
    do        
        /usr/bin/hadoop fs -rm -r -skipTrash col$I.out
    done

for PYFILE in $ls *.py
    do
    echo "running $PYFILE"
    spark-submit ./$PYFILE NYPD_Complaint_Data_Historic.csv
    done

echo "Retrieving output files from hfs"
for I in {0..22}
    do
        /usr/bin/hadoop fs -getmerge col$I.out ./output/col$I.out 
    done