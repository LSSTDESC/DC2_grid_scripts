#!/usr/bin/env python
# Python version of ImSim submission script

import sys

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

dirac = Dirac()

if len(sys.argv) < 2:
    print "Usage: submit_visit.py <visit number> [<index> <index> ...]"
    sys.exit(1)
visit = sys.argv[1]
print "Submitting jobs for visit", visit

indices = []
for i in range(2, len(sys.argv)):
    indices.append(int(sys.argv[i]))
if len(indices) == 0:
    for i in range(0, 48):
        indices.append(i)
    
#sensorfile = "/home/jperry/lsst_sensor_list.txt"

# read sensor name list
#sensorfile = open(sensorfile, 'r')
#sensorlines = sensorfile.readlines()
#sensorfile.close()
#print "Read", len(sensorlines), "sensors from sensor list"


joblistfile = open('visit_jobs_' + visit + '.txt', 'w')

# turn it into blocks
#sensorblocks = []
#i = 0
#while i < len(sensorlines):
#    block = []
#    n = len(sensorlines) - i
#    if n > 4:
#        n = 4
#    for j in range(0, n):
#        line = sensorlines[i + j].strip()
#        block.append(line)
#    sensorblocks.append(block)
#    i = i + n
#print "Read sensorblocks:", sensorblocks

#idx = 0
#for sensors in sensorblocks:
#    if len(indices) == 0 or (idx in indices):
for idx in indices:
    j = Job()
    j.setName("ImSim_" + visit + "_" + str(idx));
    
    instcatname = visit + ".tar.gz"
    insidename = 'phosim_cat_' + str(int(visit)) + '.txt'
    #args = visit + ' ' + insidename + ' "' + sensorstring + '" 4'

    startsensor = idx * 4
    numsensors = 4
    if idx == 47:
        numsensors = 1
    
    args = visit + ' ' + insidename + ' ' + str(startsensor) + ' ' + str(numsensors) + ' ' + str(idx)
    outputname = 'fits_' + visit + '_' + str(idx) + '.tar'
    
    j.setCPUTime(1209600)
    j.setExecutable('runimsim2.1.sh', arguments=args)
    j.stderr="std.err"
    j.stdout="std.out"
    #!!! May need the 2.1i directory here depending on visit number !!!
    j.setInputSandbox(["runimsim2.1.sh","run_imsim_nersc.py","LFN:/lsst/user/j/james.perry/instcats/2.1.1i/" + instcatname])
    j.setOutputSandbox(["std.out","std.err"])
    j.setTag(["4Processors"])
    j.setOutputData([visit + "/" + outputname], outputPath="", outputSE=["UKI-NORTHGRID-LANCS-HEP-disk"])
    j.setPlatform("AnyPlatform")
    j.setBannedSites(["VAC.UKI-NORTHGRID-MAN-HEP.uk", "LCG.IN2P3-CC.fr"])
    
    #print("Would submit job for sensors", sensorstring)
    jobID = dirac.submitJob(j)
    print("Submitted job as ID " + str(jobID))
    print "Status is:", dirac.status(jobID['JobID'])
    
    joblistfile.write(str(jobID['JobID']) + '\n')


joblistfile.close()
