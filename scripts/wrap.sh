#! /bin/bash

## exec 1>> /mnt/logs/vcf_populate.log
## exec 2>&1
## 
## echo "START $(date)"
## for d in /mnt/x_vcf/tmp/* ; do
## 	if [ ! -d $d ] ; then
## 		echo "Not a folder $d, skipping"
## 		continue
## 	fi
## 	echo "$(date) start processing folder $d"
##         DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_vcf_script.r
##         STATUS=$?
##         echo "$(date) processed $d exit status: $STATUS"
## done
## echo "STOP $(date)"

exec 1>> /mnt/logs/cov_populate.log
exec 2>&1

echo "START $(date)"
for d in /mnt/x_cov/tmp/* ; do
	if [ ! -d $d ] ; then
		echo "Not a folder $d, skipping"
		continue
	fi
	echo "$(date) start processing folder $d"
        DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_cov_script.r
        STATUS=$?
        echo "$(date) processed $d exit status: $STATUS"
done
echo "STOP $(date)"
