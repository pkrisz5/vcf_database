#! /bin/bash

exec 9>&1

echo "$(date) cron runs $0 $@"
SD=$(dirname $0)

###########################################
## stage 0
##  * unzip any tarbals in new folders
###########################################
python $SD/operation.py assert -s 0
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    python $SD/operation.py append -s 0 -c -1 -e '{ "command": "stg_0.sh", "arg": "x_cov" }'
    echo "$(date) STAGE 0 check x_cov"
    $SD/stg_0.sh /mnt/x_cov
    STATUS=$?
    N=$(find /mnt/x_cov/tmp -type f | wc -w)
    echo "$(date) finished processing new tarbals in x_cov/new. To load $N files. Exit status: $STATUS"
    python $SD/operation.py append -s 0 -c $STATUS -e '{ "command": "stg_0.sh", "arg": "x_cov", "n_files_cov": $N }'
fi

python $SD/operation.py assert -s 0
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    python $SD/operation.py append -s 0 -c -1 -e '{ "command": "stg_0.sh", "arg": "x_vcf" }'
    echo "$(date) STAGE 0 check x_vcf"
    $SD/stg_0.sh /mnt/x_vcf
    STATUS=$?
    N=$(find /mnt/x_vcf/tmp -type f | wc -w)
    echo "$(date) finished processing new tarbals in x_vcf/new. To load $N files. Exit status: $STATUS"
    python $SD/operation.py append -s 0 -c $STATUS -e '{ "command": "stg_0.sh", "arg": "x_vcf", "n_files_vcf": $N }'
fi

python $SD/operation.py assert -s 0
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    N1=$(find /mnt/x_cov/tmp -type f | wc -w)
    N2=$(find /mnt/x_vcf/tmp -type f | wc -w)
    N=$(( $N1 + $N2 ))
    if [ $N -gt 0 ] ; then
        python $SD/operation.py append -s 1 -c 0 -e '{ "command": "wrap.sh", "n_files_extracted": $N }'
        echo "$(date) STAGE 0: $N files in tmp folders extractes; set next stage"
    fi
fi


###########################################
## stage 1
##  * create *_append tables
###########################################
python $SD/operation.py assert -s 1
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 1 create _append tables"
    python $SD/operation.py append -s 1 -c -1 -e '{ "command": "init_db.py", "arg": "create_tables_append" }'
    python $SD/init_db.py --create_tables_append
    STATUS=$?
    echo "$(date) finished preparation. Exit status: $STATUS"
    python $SD/operation.py append -s 1 -c $STATUS -e '{ "command": "init_db.py", "arg": "create_tables_append" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 2 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 1: no issues; set next stage"
    fi
fi


###########################################
## stage 2
## populate data in tables:
##  * vcf
##  * cov
##  * meta
##  * lineage_def
###########################################
python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 2 populate vcf"
    exec 1>> /mnt/logs/vcf_populate.log 2>&1
    echo "START $(date)"
    for d in /mnt/x_vcf/tmp/* ; do
    	if [ ! -d $d ] ; then
    		echo "Not a folder $d, skipping"
    		continue
    	fi
        N=$(find $d -type f | wc -w)
	if [ $N -eq 0 ] ; then
    		echo "Empty folder $d, removing and skipping"
		rmdir $d
    		continue
	fi
    	echo "$(date) start processing folder $d"
        python $SD/operation.py append -s 2 -c -1 -e '{ "command": "ebi_vcf_script.r", "DIR_TMP": $d, "n_files": $N }'
        DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_vcf_script.r
        STATUS=$?
        echo "$(date) processed $d exit status: $STATUS"
        python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "ebi_vcf_script.r", "DIR_TMP": $d, "n_files": $N }'
	rmdir $d
    done
    echo "STOP $(date)"
    exec 1>&9 
fi

python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 2 populate cov"
    exec 1>> /mnt/logs/cov_populate.log 2>&1
    echo "START $(date)"
    for d in /mnt/x_cov/tmp/* ; do
    	if [ ! -d $d ] ; then
    		echo "Not a folder $d, skipping"
    		continue
    	fi
        N=$(find $d -type f | wc -w)
	if [ $N -eq 0 ] ; then
    		echo "Empty folder $d, removing and skipping"
		rmdir $d
    		continue
	fi
    	echo "$(date) start processing folder $d"
        python $SD/operation.py append -s 2 -c -1 -e '{ "command": "ebi_cov_script.r", "DIR_TMP": $d, "n_files": $N }'
        DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_cov_script.r
        STATUS=$?
        echo "$(date) processed $d exit status: $STATUS"
        python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "ebi_cov_script.r", "DIR_TMP": $d, "n_files": $N }'
	rmdir $d
	if [ $STATUS -neq 0 ] ; then
		break
	fi
    done
    echo "STOP $(date)"
    exec 1>&9 
fi

python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 2 populate meta"
    exec 1>> /mnt/logs/meta_populate.log 2>&1
    echo "START $(date)"
    python $SD/operation.py append -s 2 -c -1 -e '{ "command": "ebi_meta_script.r" }'
    Rscript /mnt/repo/scripts/ebi_meta_script.r
    STATUS=$?
    echo "STOP $(date) exit status: $STATUS"
    python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "ebi_meta_script.r" }'
    exec 1>&9 
fi

#python $SD/operation.py assert -s 2
#STATUS=$?
#if [ $STATUS -eq 0 ] ; then
#    echo "$(date) STAGE 2 populate lineage_def"
#    exec 1>> /mnt/logs/lineage_def.log 2>&1
#    echo "START $(date)"
#    python $SD/operation.py append -s 2 -c -1 -e '{ "command": "lineage_def_script.R" }'
#    Rscript /mnt/repo/scripts/lineage_def_script.R
#    STATUS=$?
#    echo "STOP $(date) exit status: $STATUS"
#    python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "lineage_def_script.R" }'
#    exec 1>&9 
#fi

#flip stage
python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    NR1=$(python $SD/operation.py newrecords --source cov)
    NR2=$(python $SD/operation.py newrecords --source vcf)
    NR=$(( $NR1 + $NR2 ))
    if [ $STATUS -eq 0 -a $NR -gt 0 ] ; then
        python $SD/operation.py append -s 3 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 2: no issues; set next stage"
    fi
fi

###########################################
## stage 3
###########################################
python $SD/operation.py assert -s 3
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 3 create indexes"
    python $SD/operation.py append -s 3 -c -1 -e '{ "command": "init_db.py", "arg": "create_indexes" }'
    $SD/init_db.py --create_indexes -A
    STATUS=$?
    echo "$(date) finished creating indexes. Exit status: $STATUS"
    python $SD/operation.py append -s 3 -c $STATUS -e '{ "command": "init_db.py", "arg": "create_indexes" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 4 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 3: no issues; set next stage"
    fi
fi


###########################################
## stage 4
###########################################
python $SD/operation.py assert -s 4
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 4 create materialized views"
    python $SD/operation.py append -s 4 -c -1 -e '{ "command": "init_db.py", "arg": "create_materialized_views" }'
    $SD/init_db.py --create_materialized_views -A
    STATUS=$?
    echo "$(date) finished creating materialized views. Exit status: $STATUS"
    python $SD/operation.py append -s 4 -c $STATUS -e '{ "command": "init_db.py", "arg": "create_materialized_views" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 5 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 4: no issues; set next stage"
    fi
fi


###########################################
## stage 5
###########################################
python $SD/operation.py assert -s 5
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 5 rename tables"
    python $SD/operation.py append -s 5 -c -1 -e '{ "command": "init_db.py", "arg": "rename_tables" }'
    $SD/init_db.py --rename_tables -A
    STATUS=$?
    echo "$(date) finished renaming tables. Exit status: $STATUS"
    python $SD/operation.py append -s 5 -c $STATUS -e '{ "command": "init_db.py", "arg": "rename_tables" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 6 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 5: no issues; set next stage"
    fi
fi


###########################################
## stage 6
###########################################
python $SD/operation.py assert -s 6
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 6 grant read user access"
    python $SD/operation.py append -s 6 -c -1 -e '{ "command": "init_db.py", "arg": "grant_access" }'
    $SD/init_db.py --grant_access
    STATUS=$?
    echo "$(date) finished granting access. Exit status: $STATUS"
    python $SD/operation.py append -s 6 -c $STATUS -e '{ "command": "init_db.py", "arg": "grant_access" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 0 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 6: no issues; back to STAGE 0"
    fi
fi


###########################################

exec 9>&-
echo "$(date) cron finishes running $0 $@"
