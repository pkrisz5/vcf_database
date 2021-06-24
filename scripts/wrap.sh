#! /bin/bash

echo "$(date) cron runs $0 $@"

echo "$(date) populate vcf" > /dev/stdout
exec 1>> /mnt/logs/vcf_populate.log
exec 2>&1
echo "START $(date)"
for d in /mnt/x_vcf/tmp/* ; do
	if [ ! -d $d ] ; then
		echo "Not a folder $d, skipping"
		continue
	fi
	echo "$(date) start processing folder $d"
        DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_vcf_script.r
        STATUS=$?
        echo "$(date) processed $d exit status: $STATUS"
done
echo "STOP $(date)"

## echo "$(date) populate cov" > /dev/stdout
## exec 1>> /mnt/logs/cov_populate.log
## exec 2>&1
## echo "START $(date)"
## for d in /mnt/x_cov/tmp/* ; do
## 	if [ ! -d $d ] ; then
## 		echo "Not a folder $d, skipping"
## 		continue
## 	fi
## 	echo "$(date) start processing folder $d"
##         DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_cov_script.r
##         STATUS=$?
##         echo "$(date) processed $d exit status: $STATUS"
## done
## echo "STOP $(date)"

echo "$(date) populate meta" > /dev/stdout
exec 1>> /mnt/logs/meta_populate.log
exec 2>&1
echo "START $(date)"
Rscript /mnt/repo/scripts/ebi_meta_script.r
STATUS=$?
echo "STOP $(date) exit status: $STATUS"

echo "$(date) cron finishes running $0 $@" > /dev/stdout
