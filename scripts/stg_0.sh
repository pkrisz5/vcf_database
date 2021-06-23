#! /bin/bash

R=$1
LOG=/mnt/logs/$(basename $R).log

if [ ! -d $R/new ] ; then
	echo "$R/new is missing" >&2
	echo "$0 <directory>" >&2
	exit 1
fi

if [ ! -d $R/tmp ] ; then
	echo "$R/tmp is missing" >&2
	echo "$0 <directory>" >&2
	exit 1
fi

if [ ! -d $R/archive ] ; then
	echo "$R/archive is missing" >&2
	echo "$0 <directory>" >&2
	exit 1
fi
SD=$(pwd)

cd $R/tmp
for f in ../new/*gz ; do
	[ -f $f ] || continue
	echo "$(date) extracting new file $f" >> $LOG
	tar xfz $f
	echo "$(date) move file $f in archive" >> $LOG
	mv $f ../archive
	echo "$(date) loop next" >> $LOG
done

echo "$(date) gunzip individual files" >> $LOG
find -type f -name \*.gz -exec gunzip -f {} \;
echo "$(date) finished with gunzip" >> $LOG

cd $SD
