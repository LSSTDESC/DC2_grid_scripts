#!/usr/bin/env python
#
# Look at the last N failed jobs and try to determine what happened.
# Print statistics of status, site, and possibly more detailed information gleaned
# from the outputs of the jobs.
#

import sys
import subprocess
import os

from DIRAC.Core.Base import Script
Script.parseCommandLine()
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac


# cmd is list containing command and its parameters
# returns stdout from command
def runCommand(cmd):
   from subprocess import PIPE
   session = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)
   stdout, stderr = session.communicate()
   return stdout

dirac = Dirac()

failedjoblist = "/home/jperry/jobsfailed.txt"

# decide how many jobs to analyse
count = 1000
if len(sys.argv) > 1:
   count = int(sys.argv[1])
   
# read the failed jobs list
f = open(failedjoblist, 'r')
lines = f.readlines()
f.close()

sitefailures = {}
minorstatuses = {}

jobids = []
for line in lines[-count:]:
   line = line.strip()
   bits = line.split(' ')
   
   # visit, idx, job ID, site
   jobid = int(bits[2])
   site = bits[3]
   jobids.append(jobid)
   
   # tally up failures by site
   if site in sitefailures:
      sitefailures[site] = sitefailures[site] + 1
   else:
      sitefailures[site] = 1

# get statuses
statuslist = dirac.status(jobids)
if not 'Value' in statuslist:
   print "Error getting job status from DIRAC!"
   sys.exit(1)

cvmfsProblemCount = 0

# tally up minor status
for i in jobids:
   minorstatus = statuslist['Value'][i]['MinorStatus']
   if minorstatus in minorstatuses:
      minorstatuses[minorstatus] = minorstatuses[minorstatus] + 1
   else:
      minorstatuses[minorstatus] = 1

   if minorstatus == "Application Finished With Errors":
      # investigate application errors in more detail
      print "Investigating job", i, "application error"
      os.system("dirac-wms-job-get-output " + str(i))
      f = open(str(i) + "/Script1_launch_container.sh.log", "r")
      outputlines = f.readlines()
      f.close()
      cvmfsProblem = False
      for line in outputlines:
         if line.find("Image path /cvmfs/gridpp.egi.eu/lsst/containers/Run2.2i-production-v2/ doesn't exist") >= 0:
            print("CVMFS directory not found!")
            cvmfsProblem = True
            cvmfsProblemCount = cvmfsProblemCount + 1
            break
      if not cvmfsProblem:
         os.system("echo " + str(i) + " >> applicationerrors.txt")
         os.system("cat " + str(i) + "/Script1_launch_container.sh.log >> applicationerrors.txt")
      os.system("rm -rf " + str(i))
   
# print out site stats
for site in sitefailures.keys():
   print "Site", site, "had", sitefailures[site], "failed jobs"
   #
   # Application Finished With Errors
   # Maximum of reschedulings reached
   # Job stalled: pilot not running
   # Stalling for more than 34200 sec
   # Watchdog identified this job as stalled
   #
   afwe = 0
   morr = 0
   jspnr = 0
   sfmt34200s = 0
   witjas = 0
   other = 0
   # look for jobs at this site
   for line in lines[-count:]:
      line = line.strip()
      bits = line.split(' ')
      jobid = int(bits[2])
      jsite = bits[3]
      if jsite == site:
         minorstatus = statuslist['Value'][jobid]['MinorStatus']
         if minorstatus == 'Application Finished With Errors':
            afwe = afwe + 1
         elif minorstatus == 'Maximum of reschedulings reached':
            morr = morr + 1
         elif minorstatus == 'Job stalled: pilot not running':
            jspnr = jspnr + 1
         elif minorstatus == 'Stalling for more than 34200 sec':
            sfmt34200s = sfmt34200s + 1
         elif minorstatus == 'Watchdog identified this job as stalled':
            witjas = witjas + 1
         else:
            other = other + 1
   print "  Application Finished With Errors:", afwe
   print "  Maximum of reschedulings reached:", morr
   print "  Job stalled: pilot not running:  ", jspnr
   print "  Stalling for more than 34200 sec:", sfmt34200s
   print "  Watchdog identified as stalled:  ", witjas
   print "  Other:                           ", other
   print ""

# print out minor status stats
for minorstatus in minorstatuses.keys():
   print "Minor status", minorstatus, "accounted for", minorstatuses[minorstatus], "failed jobs"

print "CVMFS problem accounted for", cvmfsProblemCount, "failed jobs"   
