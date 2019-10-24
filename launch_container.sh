#!/bin/bash
# Arguments:
#  - visit number (always full 8 digits)
#  - name of actual top level catalogue file within archive
#  - index of first sensor to run (0-188)
#  - number of processes to run
#  - index number of job within visit

# extract the instance catalogue
tar xzf ${1}.tar.gz

# create a home directory for the container
mkdir home

# launch the container
singularity exec -H `pwd`/home -B `pwd`:/projects/LSSTsky:rw /cvmfs/gridpp.egi.eu/lsst/imsim_sandbox/ /projects/LSSTsky/docker_run.sh python run_imsim_nersc.py --processes $4 --subset_index $3 --subset_size $4 --instcat $2 --outdir fits --config /projects/LSSTsky/parsl_imsim_configs

result=$?
if [ $result -ne 0 ] ; then
    exit $result
else
    # package up output
    outputname=fits_${1}_${5}.tar
    if [ -d fits ] ; then
        tar cf $outputname fits/
    else
        echo "Job produced no output!"
        mkdir fits # create dummy data so job doesn't fail
        echo "Dummy data" > fits/file.txt
        tar cf $outputname fits/
    fi
fi

