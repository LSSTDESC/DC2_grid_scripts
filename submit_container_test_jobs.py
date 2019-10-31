#!/usr/bin/env python
# Submits the same container job to each of the sites for testing
# Arguments: visit number (8 digits); index; optional site (for single site)
# The job IDs are recorded in a file named 'container_site_jobs.txt'.

import sys

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

sites = [ "VAC.UKI-NORTHGRID-MAN-HEP.uk", "LCG.IN2P3-CC.fr", "LCG.RAL-LCG2.uk", "LCG.UKI-LT2-IC-HEP.uk", "LCG.UKI-NORTHGRID-LANCS-HEP.uk", "LCG.UKI-NORTHGRID-MAN-HEP.uk", "LCG.UKI-SCOTGRID-ECDF.uk", "LCG.IN2P3-LAPP.fr", "LCG.UKI-LT2-Brunel.uk", "LCG.UKI-LT2-QMUL.uk", "LCG.UKI-NORTHGRID-LIV-HEP.uk", "LCG.UKI-SOUTHGRID-BRIS-HEP.uk", "LCG.UKI-SOUTHGRID-RALPP.uk" ]

dirac = Dirac()

if len(sys.argv) < 3:
    print "Usage: submit_container_test_jobs.py <visit number> <index> [<site>]"
    sys.exit(1)
visit = sys.argv[1]
idx = int(sys.argv[2])
print "Submitting jobs for visit", visit, "index", idx

listfile = "container_site_jobs.txt"

if len(sys.argv) == 4:
    sites = [sys.argv[3]]
    listfile = "container_test_" + sys.argv[3] + ".txt"
    
# open a file to record a list of job IDs
joblistfile = open(listfile, 'w')

for site in sites:
    j = Job()
    j.setName("ImSim_" + visit + "_" + str(idx));
    
    instcatname = visit + ".tar.gz"
    insidename = 'phosim_cat_' + str(int(visit)) + '.txt'

    #startsensor = idx * 4
    #numsensors = 4
    #if idx == 47:
    #   numsensors = 1
    startsensor = idx
    numsensors = 1
    
    args = visit + ' ' + insidename + ' ' + str(startsensor) + ' ' + str(numsensors) + ' ' + str(idx)
    outputname = 'fits_' + visit + '_' + str(idx) + '.tar'
    
    j.setCPUTime(1209600)
    j.setExecutable('launch_container.sh', arguments=args)
    j.stderr="std.err"
    j.stdout="std.out"
    #!!! May need the 2.1i directory here depending on visit number !!!
    j.setInputSandbox(["launch_container.sh","docker_run.sh","run_imsim_nersc.py","parsl_imsim_configs","finals2000A.all","LFN:/lsst/user/j/james.perry/2.2i_test/" + instcatname])
    j.setOutputSandbox(["std.out","std.err"])
    j.setTag(["8Processors"])
    j.setOutputData([outputname], outputPath="", outputSE=["UKI-NORTHGRID-LANCS-HEP-disk"])
    j.setPlatform("EL7")

    j.setDestination(site)
    
    jobID = dirac.submitJob(j)
    print("Submitted job as ID " + str(jobID))
    print "Status is:", dirac.status(jobID['JobID'])
    
    joblistfile.write(str(jobID['JobID']) + '\n')


joblistfile.close()
