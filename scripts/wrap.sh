#! /bin/bash

alias python=python3
SD=$(dirname $0)

echo "$(date) cron runs $0 $@"
exec 9>&1
exec 8>&2

###########################################
## stage 0
##  * unzip any tarbals in new folders
###########################################
exec 1>> /mnt/logs/stage_0.log
exec 2>&1

python $SD/operation.py assert -s 0
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    python $SD/operation.py append -s 0 -c -1 -e '{ "command": "stg_0.sh", "arg": "x_cov" }'
    echo "$(date) STAGE 0 checking for new tarbals in x_cov/new" | tee >&9
    $SD/stg_0.sh /mnt/x_cov
    STATUS=$?
    N=$(find /mnt/x_cov/tmp -type f | wc -w)
    python $SD/operation.py append -s 0 -c $STATUS -e '{ "command": "stg_0.sh", "arg": "x_cov", "n_files_cov": '$N' }'
    echo "$(date) finished processing new tarbals in x_cov/new. $N files to process later. Exit status: $STATUS" | tee >&9
fi

python $SD/operation.py assert -s 0
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    python $SD/operation.py append -s 0 -c -1 -e '{ "command": "stg_0.sh", "arg": "x_vcf" }'
    echo "$(date) STAGE 0 checking for new tarbals in x_vcf/new" | tee >&9
    $SD/stg_0.sh /mnt/x_vcf
    STATUS=$?
    N=$(find /mnt/x_vcf/tmp -type f | wc -w)
    python $SD/operation.py append -s 0 -c $STATUS -e '{ "command": "stg_0.sh", "arg": "x_vcf", "n_files_vcf": '$N' }'
    echo "$(date) finished processing new tarbals in x_vcf/new. $N files to process later. Exit status: $STATUS" | tee >&9
fi

python $SD/operation.py assert -s 0
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    N1=$(find /mnt/x_cov/tmp -type f | wc -w)
    N2=$(find /mnt/x_vcf/tmp -type f | wc -w)
    N=$(( $N1 + $N2 ))
    if [ $N -gt 0 ] ; then
        python $SD/operation.py append -s 1 -c 0 -e '{ "command": "wrap.sh", "n_files_extracted": '$N' }'
        echo "$(date) STAGE 0->1: Alltogether $N files extracted in both tmp folders" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
## stage 1
##  * create *_append tables
###########################################
exec 1>> /mnt/logs/stage_1.log
exec 2>&1

python $SD/operation.py assert -s 1
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 1 create _append tables" | tee >&9
    python $SD/operation.py append -s 1 -c -1 -e '{ "command": "init_db.py", "arg": "create_tables_append" }'
    python $SD/init_db.py --create_tables_append
    STATUS=$?
    echo "$(date) finished preparation. Exit status: $STATUS"
    python $SD/operation.py append -s 1 -c $STATUS -e '{ "command": "init_db.py", "arg": "create_tables_append" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 2 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 1->2: no issues" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
## stage 2
## populate data in tables:
##  * vcf
##  * cov
##  * meta
##  * lineage_def
###########################################
exec 1>> /mnt/logs/stage_2.log
exec 2>&1

python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 2 populate vcf" | tee >&9
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
    	echo "$(date) start processing folder $d" | tee >&9
        python $SD/operation.py append -s 2 -c -1 -e '{ "command": "ebi_vcf_script.r", "DIR_TMP": '$d', "n_files": '$N' }'
        DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_vcf_script.r
        STATUS=$?
        echo "$(date) processed $d exit status: $STATUS" | tee >&9
        python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "ebi_vcf_script.r", "DIR_TMP": '$d', "n_files": '$N' }'
	rmdir $d
    done
fi

python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 2 populate cov" | tee >&9
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
    	echo "$(date) start processing folder $d" | tee >&9
        python $SD/operation.py append -s 2 -c -1 -e '{ "command": "ebi_cov_script.r", "DIR_TMP": '$d', "n_files": '$N' }'
        DIR_TMP=$d/ Rscript /mnt/repo/scripts/ebi_cov_script.r
        STATUS=$?
        echo "$(date) processed $d exit status: $STATUS" | tee >&9
        python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "ebi_cov_script.r", "DIR_TMP": '$d', "n_files": '$N' }'
	rmdir $d
    done
fi

python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 2 populate meta" | tee >&9
    python $SD/operation.py append -s 2 -c -1 -e '{ "command": "ebi_meta_script.r" }'
    Rscript /mnt/repo/scripts/ebi_meta_script.r
    STATUS=$?
    echo "$(date) download of meta information exit status: $STATUS" | tee >&9
    python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "ebi_meta_script.r" }'
fi

#python $SD/operation.py assert -s 2
#STATUS=$?
#if [ $STATUS -eq 0 ] ; then
#    echo "$(date) STAGE 2 populate lineage_def" | tee >&9
#    python $SD/operation.py append -s 2 -c -1 -e '{ "command": "lineage_def_script.R" }'
#    Rscript /mnt/repo/scripts/lineage_def_script.R
#    STATUS=$?
#    echo "$(date) inserting lineage_def exit status: $STATUS" | tee >&9
#    python $SD/operation.py append -s 2 -c $STATUS -e '{ "command": "lineage_def_script.R" }'
#fi

python $SD/operation.py assert -s 2
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    NR1=$(python $SD/operation.py newrecords --source cov)
    NR2=$(python $SD/operation.py newrecords --source vcf)
    NR=$(( $NR1 + $NR2 ))
    if [ $NR -gt 0 ] ; then
        python $SD/operation.py append -s 3 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 2->3: no issues" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
## stage 3
###########################################
exec 1>> /mnt/logs/stage_3.log
exec 2>&1

python $SD/operation.py assert -s 3
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 3 create indexes" | tee >&9
    python $SD/operation.py append -s 3 -c -1 -e '{ "command": "init_db.py", "arg": "create_indexes" }'
    python $SD/init_db.py --create_indexes -A
    STATUS=$?
    echo "$(date) finished creating indexes. Exit status: $STATUS" | tee >&9
    python $SD/operation.py append -s 3 -c $STATUS -e '{ "command": "init_db.py", "arg": "create_indexes" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 4 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 3->4: no issues" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
## stage 4
###########################################
exec 1>> /mnt/logs/stage_4.log
exec 2>&1

python $SD/operation.py assert -s 4
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 4 create materialized views" | tee >&9
    python $SD/operation.py append -s 4 -c -1 -e '{ "command": "init_db.py", "arg": "create_materialized_views" }'
    python $SD/init_db.py --create_materialized_views -A
    STATUS=$?
    echo "$(date) finished creating materialized views. Exit status: $STATUS" | tee >&9
    python $SD/operation.py append -s 4 -c $STATUS -e '{ "command": "init_db.py", "arg": "create_materialized_views" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 5 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 4->5: no issues" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
## stage 5
###########################################
exec 1>> /mnt/logs/stage_5.log
exec 2>&1

python $SD/operation.py assert -s 5
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 5 rename tables" | tee >&9
    python $SD/operation.py append -s 5 -c -1 -e '{ "command": "init_db.py", "arg": "rename_tables" }'
    python $SD/init_db.py --rename_tables -A
    STATUS=$?
    echo "$(date) finished renaming tables. Exit status: $STATUS" | tee >&9
    python $SD/operation.py append -s 5 -c $STATUS -e '{ "command": "init_db.py", "arg": "rename_tables" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 6 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 5->6: no issues" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
## stage 6
###########################################
exec 1>> /mnt/logs/stage_6.log
exec 2>&1

python $SD/operation.py assert -s 6
STATUS=$?
if [ $STATUS -eq 0 ] ; then
    echo "$(date) STAGE 6 grant read user access" | tee >&9
    python $SD/operation.py append -s 6 -c -1 -e '{ "command": "init_db.py", "arg": "grant_access" }'
    python $SD/init_db.py --grant_access
    STATUS=$?
    echo "$(date) finished granting access. Exit status: $STATUS" | tee >&9
    python $SD/operation.py append -s 6 -c $STATUS -e '{ "command": "init_db.py", "arg": "grant_access" }'
    if [ $STATUS -eq 0 ] ; then
        python $SD/operation.py append -s 0 -c 0 -e '{ "command": "wrap.sh" }'
        echo "$(date) STAGE 6->0: no issues" | tee >&9
    fi
fi

exec 1>&9
exec 2>&8


###########################################
exec 9>&-
exec 2>&-
echo "$(date) cron finishes running $0 $@"
