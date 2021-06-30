#! /bin/bash

function usage {
    echo "ERROR: $1" >&2 
    echo "$0 <directory>" >&2
    exit $2
}

[ $# -eq 1 ] || usage "Run with exactly one command line argument pointing to data folder" 1
R=$1

[ -d $R/new ] || usage "Missing folder: $R/new" 2
[ -d $R/tmp ] || usage "Missing folder: $R/tmp" 2
[ -d $R/archive ] || usage "Missing folder: $R/archive" 2

SD=$(pwd)

cd $R/tmp
for f in ../new/*gz ; do
	[ -f $f ] || continue
	echo "$(date) extracting new file $f"
	tar xfz $f
	echo "$(date) move file $f in archive"
	mv $f ../archive
	echo "$(date) loop next"
done

echo "$(date) gunzip individual files"
find -type f -name \*.gz -exec gunzip -f {} \;
echo "$(date) finished with gunzip"

cd $SD
