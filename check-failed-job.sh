#!/bin/bash

dirac-wms-job-status $1
dirac-wms-job-get-output $1
cat $1/Script1_launch_container.sh.log
rm -rf $1

