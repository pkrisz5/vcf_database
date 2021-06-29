#! /bin/bash

exec 9>&1

echo "$(date) cron runs $0 $@"
SD=$(dirname $0)

###########################################
## stage 0
##  * unzip any tarbals in new folders
###########################################
echo "$(date) STAGE 0 check x_cov"
$SD/stg_0.sh /mnt/x_cov
STATUS=$?
N=$(find /mnt/x_cov/tmp -type f | wc -w)
echo "$(date) finished processing new tarbals in x_cov/new. To load $N files. Exit status: $STATUS"

echo "$(date) STAGE 0 check x_vcf"
$SD/stg_0.sh /mnt/x_vcf
STATUS=$?
N=$(find /mnt/x_vcf/tmp -type f | wc -w)
echo "$(date) finished processing new tarbals in x_vcf/new. To load $N files. Exit status: $STATUS"

###########################################
## stage 1
##  * create *_append tables
###########################################
echo "$(date) STAGE 1 create _append tables"
$SD/init_db.py --create_tables_append
STATUS=$?
echo "$(date) finished preparation. Exit status: $STATUS"


###########################################
## stage 2
## populate data in tables:
##  * vcf
##  * cov
##  * meta
##  * lineage_def
###########################################
echo "$(date) STAGE 2 populate vcf"
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

echo "$(date) STAGE 2 populate cov"
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

echo "$(date) STAGE 2 populate meta"
exec 1>> /mnt/logs/meta_populate.log 2>&1
echo "START $(date)"
Rscript /mnt/repo/scripts/ebi_meta_script.r
STATUS=$?
echo "STOP $(date) exit status: $STATUS"
exec 1>&9 

echo "$(date) STAGE 2 populate lineage_def"
exec 1>> /mnt/logs/lineage_def.log 2>&1
echo "START $(date)"
Rscript /mnt/repo/scripts/lineage_def_script.R
STATUS=$?
echo "STOP $(date) exit status: $STATUS"
exec 1>&9 


###########################################
## stage 3
###########################################
echo "$(date) STAGE 3 create indexes"
$SD/init_db.py --create_indexes -A
STATUS=$?
echo "$(date) finished creating indexes. Exit status: $STATUS"


###########################################
## stage 4
###########################################
echo "$(date) STAGE 4 create materialized views"
## init_db.py rename


###########################################
## stage 5
###########################################
echo "$(date) STAGE 5 create materialized views"
$SD/init_db.py --create_materialized_views -A
STATUS=$?
echo "$(date) finished creating materialized views. Exit status: $STATUS"


###########################################
## stage 6
###########################################
echo "$(date) STAGE 6 rename tables"
$SD/init_db.py --rename_tables -A
STATUS=$?
echo "$(date) finished renaming tables. Exit status: $STATUS"


echo "$(date) grant read user access"
$SD/init_db.py --grant_access
STATUS=$?
echo "$(date) finished granting access. Exit status: $STATUS"

exec 9>&-
echo "$(date) cron finishes running $0 $@"
