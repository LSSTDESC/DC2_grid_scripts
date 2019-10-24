#!/usr/bin/env python


from DIRAC.Core.Base import Script
Script.parseCommandLine()
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

joblistfile = "/home/jperry/joblist.txt"

# returns a list of tuples. each one contains:
#  - visit (6 digit string)
#  - index (int)
#  - Dirac job ID (int)
def readJobList(filename):
    listfile = open(filename, 'r')
    lines = listfile.readlines()
    listfile.close()
    result = []
    for line in lines:
        line = line.strip()
        bits = line.split(' ')
        result.append( (bits[0], int(bits[1]), int(bits[2])) )
    return result

# start up Dirac first and create an instance
dirac = Dirac()

# read master job list
joblist = readJobList(joblistfile)
print "Read", len(joblist), "jobs from job list"


# make a list of job IDs so we can cancel them all at once
jobidlist = []
for i in joblist:
    #if i[2] >= 15870062:
    jobidlist.append(i[2])

dirac.deleteJob(jobidlist)
