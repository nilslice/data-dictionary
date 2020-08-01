#!/bin/bash

# ./pubsub-load.sh 50 .json.gz private dataset_name -s
echo "count of files=$1 file_ext=$2 classification=$3 dataset_name=$4 run_in_serial=$5"

echo "some data" > base_partition$2
for i in $(seq 1 $1); do
    PARTITION="base_partition_${i}$2"
    cp base_partition$2 $PARTITION
done
echo "finished writing files... uploading"
case "$5" in
	-serial|-s)
		gsutil cp base_partition_*$2 gs://nilslice-datasets-dev-$3/$4/	
		;;
	*)
		gsutil -m cp base_partition_*$2 gs://nilslice-datasets-dev-$3/$4/
		;;
esac
rm base_partition*$2
