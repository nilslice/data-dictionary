#!/bin/bash

echo "some data" > base_partition.txt
for i in $(seq 1 $1); do
    PARTITION="base_partition_${i}.txt"
    cp base_partition.txt $PARTITION
done
echo "finished writing files... uploading"
case "$3" in
	-serial|-s)
		gsutil cp base_partition_*.txt gs://nilslice-datasets-dev-public/$2/
		sleep 1
		;;
	*)
		gsutil -m cp base_partition_*.txt gs://nilslice-datasets-dev-public/$2/
		;;
esac
rm base_partition*.txt
