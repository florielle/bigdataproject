echo "Removing output files from hfs"
for I in {0..22}
    do        
        /usr/bin/hadoop fs -rm -r -skipTrash col$I_counts.out
    done

for I in {0..22}
    do
    echo "running col$I counts"
    /usr/bin/hadoop jar /opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hadoop-mapreduce/hadoop-streaming.jar -files "map_reduce/" -mapper "map_reduce/map.py" -reducer "map_reduce/reduce.py" -input "col$I.out" -output "col$I_counts.out"
    done

echo "Retrieving output files from hfs"
for I in {0..22}
    do
        /usr/bin/hadoop fs -getmerge col$I_counts.out ./output/col$I_counts.out 
    done