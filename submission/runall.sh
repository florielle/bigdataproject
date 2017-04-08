rm -r -f output
mkdir output

echo "Removing output files from hfs"
for I in {1..22}
    do        
        /usr/bin/hadoop fs -rm -r -skipTrash col$I.out
    done

for PYFILE in $ls *.py
    do
    echo "running $PYFILE"
    spark-submit ./$PYFILE NYPD_Complaint_Data_Historic
    done

for I in {1..22}
    do
        echo "Retrieving output files from hfs"
        /usr/bin/hadoop fs -getmerge col$I.out col$I.out 
    done