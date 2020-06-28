#!/bin/bash

echo "some data" > base_partition.txt
for i in $(seq 1 $1); do
    PARTITION="base_partition_${i}.txt"
    cp base_partition.txt $PARTITION
done
echo "finished writing files... uploading"
gsutil -m cp base_partition_*.txt gs://nilslice-datasets-dev-public/$2/
rm base_partition*.txt