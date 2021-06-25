#! /bin/bash

exec 9>&1

echo "$(date) cron runs $0 $@"
SD=$(dirname $0)

###########################################
## stage 0
##  * unzip any tarbals in new folders
###########################################
echo "$(date) check x_cov"
$SD/stg_0.sh /mnt/x_cov
echo "$(date) check x_vcf"
$SD/stg_0.sh /mnt/x_vcf

###########################################
## stage 1
##  * 
###########################################
echo "$(date) TODO stage 1"

###########################################
## stage 2
## populate data in tables:
##  * vcf
##  * cov
##  * meta
##  * lineage_def
###########################################
echo "$(date) populate vcf"
exec 1>> /mnt/logs/vcf_populate.log 2>&1
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
exec 1>&9 

echo "$(date) populate cov"
exec 1>> /mnt/logs/cov_populate.log 2>&1
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
exec 1>&9 

echo "$(date) populate meta"
exec 1>> /mnt/logs/meta_populate.log 2>&1
echo "START $(date)"
Rscript /mnt/repo/scripts/ebi_meta_script.r
STATUS=$?
echo "STOP $(date) exit status: $STATUS"
exec 1>&9 

echo "$(date) populate lineage_def"
exec 1>> /mnt/logs/lineage_def.log 2>&1
echo "START $(date)"
Rscript /mnt/repo/scripts/lineage_def_script.R
STATUS=$?
echo "STOP $(date) exit status: $STATUS"
exec 1>&9 


exec 9>&-
echo "$(date) cron finishes running $0 $@"
