#! /bin/bash

SD=$(dirname $0)

echo "$(date) cron runs $0 $@"
exec 9>&1
exec 8>&2

###########################################
exec 1>> /mnt/logs/ecdc.log
exec 2>&1

echo "$(date) run ecdc script" | tee >&9
Rscript $SD/ecdc_covid_country_weekly_script.R
STATUS=$?
echo "$(date) finished exit status: $STATUS; granting access" | tee >&9
python3 $SD/init_db.py --grant_access
STATUS=$?
echo "$(date) finished granting access. Exit status: $STATUS" | tee >&9

exec 1>&9
exec 2>&8
###########################################
exec 9>&-
exec 2>&-
echo "$(date) cron finishes running $0 $@"

