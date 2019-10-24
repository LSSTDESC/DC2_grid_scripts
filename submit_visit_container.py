#!/usr/bin/env python
# Submits a single visit (or a subset of a visit) to the grid for processing.
# This does not integrate with the infrastructure used by jobmanager.py. Instead
# the jobs must be checked on manually.
# First argument is name of visit to submit, as an 8 digit number.
# Further arguments are optional. If any are given, they are indices (0-47) of jobs
# within that visit that should be submitted.
# If none are given, all 48 jobs are submitted.
# The job IDs are recorded in a file named 'visit_jobs_<visit>.txt'.

import sys

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

dirac = Dirac()

if len(sys.argv) < 2:
    print "Usage: submit_visit_container.py <visit number> [<index> <index> ...]"
    sys.exit(1)
visit = sys.argv[1]
print "Submitting jobs for visit", visit

indices = []
for i in range(2, len(sys.argv)):
    indices.append(int(sys.argv[i]))
if len(indices) == 0:
    for i in range(0, 48):
        indices.append(i)

# open a file to record a list of jobs for this visit
joblistfile = open('visit_jobs_' + visit + '.txt', 'w')

for idx in indices:
    j = Job()
    j.setName("ImSim_" + visit + "_" + str(idx));
    
    instcatname = visit + ".tar.gz"
    insidename = 'phosim_cat_' + str(int(visit)) + '.txt'

    startsensor = idx * 4
    numsensors = 4
    if idx == 47:
        numsensors = 1
    
    args = visit + ' ' + insidename + ' ' + str(startsensor) + ' ' + str(numsensors) + ' ' + str(idx)
    outputname = 'fits_' + visit + '_' + str(idx) + '.tar'
    
    j.setCPUTime(1209600)
    j.setExecutable('launch_container.sh', arguments=args)
    j.stderr="std.err"
    j.stdout="std.out"
    #!!! May need the 2.1i directory here depending on visit number !!!
    j.setInputSandbox(["launch_container.sh","docker_run.sh","run_imsim_nersc.py","parsl_imsim_configs","LFN:/lsst/user/j/james.perry/2.2i_test/" + instcatname])
    j.setOutputSandbox(["std.out","std.err"])
    j.setTag(["4Processors"])
    j.setOutputData([outputname], outputPath="", outputSE=["UKI-NORTHGRID-LANCS-HEP-disk"])
    j.setPlatform("AnyPlatform")

    # FIXME: remove these once those sites are working again
    j.setBannedSites(["VAC.UKI-NORTHGRID-MAN-HEP.uk", "LCG.IN2P3-CC.fr"])
    
    jobID = dirac.submitJob(j)
    print("Submitted job as ID " + str(jobID))
    print "Status is:", dirac.status(jobID['JobID'])
    
    joblistfile.write(str(jobID['JobID']) + '\n')


joblistfile.close()
