rm -r -f output
mkdir output

for I in {1..22}
    do
        echo "Removing output files from hfs"
        hfs -rm -r -skipTrash col$I.out
    done

for PYFILE in $ls *.py
    do
    echo "running $PYFILE"
    spark-submit ./$PYFILE NYPD_Complaint_Data_Historic
    done

for I in {1..22}
    do
        echo "Retrieving output files from hfs"
        hfs -getmerge col$I.out col$I.out 
    done