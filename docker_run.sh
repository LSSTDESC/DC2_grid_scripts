#!/bin/bash

cd /DC2

source /opt/lsst/software/stack/loadLSST.bash
setup lsst_sims -t sims_w_2019_42
setup -t DC2production throughputs
setup -t DC2production sims_skybrightness_data
setup -r imSim -j
#setup -r sims_GalSimInterface -j
export OMP_NUM_THREADS=1

cd /projects/LSSTsky

$@
