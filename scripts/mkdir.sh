#! /bin/bash

function _mkdir {
    if [ -d $1 ] ; then
        echo "folder $1 exists" >&2 
    else
        echo "creating $1" >&2 
	mkdir -p $1
    fi
}

_mkdir /mnt/logs
_mkdir /mnt/x_cov/new
_mkdir /mnt/x_cov/tmp
_mkdir /mnt/x_cov/archive
_mkdir /mnt/x_vcf/new
_mkdir /mnt/x_vcf/tmp
_mkdir /mnt/x_vcf/archive

