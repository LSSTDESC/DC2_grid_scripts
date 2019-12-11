#!/bin/bash
# Upload input files from Cori to grid storage via 3rd party GridFTP.
# Valid proxy and globus-url-copy installation are required.
# The files need to be registered in the DIRAC File Catalogue after uploading.

srcbase="gsiftp://dtn01.nersc.gov/global/cscratch1/sd/descim/instcat_y05_191109"
destbase="gsiftp://srm-rdf.gridpp.ecdf.ed.ac.uk/dpm/srm-rdf.gridpp.ecdf.ed.ac.uk/home/lsst/lsst/user/j/james.perry/instcats/2.2i/y05"

if [ $# -ne 1 ] ; then
    echo "Usage: upload_input_data_3rd_party.sh <list file>"
    exit 1
fi

listfile=$1

set `cat $listfile`

while [ $# -gt 0 ] ; do
    echo "Transferring file $1"
    globus-url-copy -vb ${srcbase}/${1} ${destbase}/${1}
    if [ $? -ne 0 ] ; then
        echo "Error transferring $1" >> transfer_errors.txt
    fi
    shift 1
done
