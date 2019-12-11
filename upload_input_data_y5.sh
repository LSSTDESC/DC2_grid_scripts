#!/bin/bash
# Upload input files from Cori to grid storage
# Valid proxy and DIRAC environment are required for this to work

srcbase="gsiftp://dtn01.nersc.gov/global/cscratch1/sd/descim/instcat_y05_191109"
lfnbase="/lsst/user/j/james.perry/instcats/2.2i/y05"
se=UKI-NORTHGRID-LANCS-HEP-disk

if [ $# -ne 1 ] ; then
    echo "Usage: upload_input_data.sh <list file>"
    exit 1
fi

listfile=$1

set `cat $listfile`

while [ $# -gt 0 ] ; do
    echo "Transferring file $1"
    globus-url-copy -vb ${srcbase}/${1} file://`pwd`/${1}
    if [ $? -ne 0 ] ; then
        echo "Error downloading $1" >> transfer_errors.txt
    else
        dirac-dms-add-file ${lfnbase}/${1} $1 $se
        if [ $? -ne 0 ] ; then
            echo "Error uploading $1" >> transfer_errors.txt
        fi
        rm $1
    fi
    shift 1
done
