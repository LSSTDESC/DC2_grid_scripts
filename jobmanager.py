#!/usr/bin/env python
#
# Automatic ImSim manager script using Dirac

from DIRAC.Core.Base import Script
Script.parseCommandLine()
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

import signal, os, time

# constants
# data file paths
joblistfile = "/home/jperry/joblist.txt"
completedlistfile = "/home/jperry/jobscompleted.txt"
visitlist = "/home/jperry/visitlist.txt"
visitposition = "/home/jperry/visitposition.txt"
sitelistfile = "/home/jperry/jobsites.txt"
resubmissionsfile = "/home/jperry/resubmissions.txt"

# how many jobs we should aim to have in the system at once
DESIRED_JOB_COUNT = 50000


# returns a list of tuples. each one contains:
#  - visit (8 digit string)
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

def writeJobList(filename, joblist):
    listfile = open(filename, 'w')
    for job in joblist:
        listfile.write(job[0] + " " + str(job[1]) + " " + str(job[2]) + "\n")
    listfile.close()

def removeJobFromList(l, jobId):
    for i in range(0, len(l)):
        if l[i][2] == jobId:
            del l[i]
            return

def submitImsimJob(dirac, joblist, visit, idx):
    j = Job()
    j.setName("ImSim_" + visit + "_" + str(idx));
    j.setCPUTime(1209600)

    instcatname = visit + ".tar.gz"
    insidename = 'phosim_cat_' + str(int(visit)) + '.txt'
        
    startsensor = idx * 4
    numsensors = 4
    if idx == 47:
        numsensors = 1

    args = visit + ' ' + insidename + ' ' + str(startsensor) + ' ' + str(numsensors) + ' ' + str(idx)
    outputname = 'fits_' + visit + '_' + str(idx) + '.tar'

    print "Submitting ImSim job with arguments", args
    
    j.setExecutable('runimsim2.1.sh', arguments=args)
    j.stderr="std.err"
    j.stdout="std.out"
    j.setInputSandbox(["runimsim2.1.sh","run_imsim_nersc.py","LFN:/lsst/user/j/james.perry/instcats/2.1i/" + instcatname])
    j.setOutputSandbox(["std.out","std.err"])
    j.setTag(["4Processors"])
    j.setOutputData([visit + "/" + outputname], outputPath="", outputSE=["UKI-NORTHGRID-LANCS-HEP-disk"])
    j.setPlatform("AnyPlatform")

    #j.setBannedSites(["VAC.UKI-NORTHGRID-MAN-HEP.uk", "LCG.IN2P3-CC.fr"])
    #j.setDestination("LCG.RAL-LCG2.uk")
    
    jobID = dirac.submitJob(j)

    # add to the list
    ok = jobID['OK']
    jobID = jobID['JobID']
    joblist.append( ( visit, idx, jobID ) )
    print "Adding job ID", jobID, "to list"
    return ok

# submit a whole batch of jobs for a complete visit
def submitBatch(dirac, joblist, visit):
    print "*** Submitting jobs for", visit, "***"
    for i in range(0, 48):
        submitImsimJob(dirac, joblist, visit, i)


exitnow = False

def signalhandler(signum, frame):
    global exitnow
    print "Caught SIGINT, exiting after this iteration"
    exitnow = True
    

# start up Dirac first and create an instance
dirac = Dirac()

# read master job list
joblist = readJobList(joblistfile)
print "Read", len(joblist), "jobs from job list"

# read visits file
visitsfile = open(visitlist, 'r')
visitslines = visitsfile.readlines()
visitsfile.close()
visits = []
for line in visitslines:
    visits.append(line.strip())

# read input visit position and work out where we are
visitposfile = open(visitposition, 'r')
visitpos = int(visitposfile.read().strip())
visitposfile.close()

# install signal handler so we don't lose data when closing
signal.signal(signal.SIGINT, signalhandler)
signal.signal(signal.SIGTERM, signalhandler)

# run any resubmissions required
if os.path.exists(resubmissionsfile):
    resubmissions = readJobList(resubmissionsfile)
    for resub in resubmissions:
        success = False
        while not success:
            try:
                submitImsimJob(dirac, joblist, resub[0], resub[1])
                success = True
            except:
                print "Resubmission failed, trying again..."
                pass

# enter main loop
while not exitnow:
    print "Running main loop"

    # check job statuses
    failedlist = []
    completedlist = []
    sitelist = []
    submittedjobs = 0
    runningjobs = 0
    otherjobs = 0

    # make a list of job IDs so we can request all the statuses at once from Dirac
    jobidlist = []
    for i in joblist:
        jobidlist.append(i[2])

    statuslist = dirac.status(jobidlist)
    if not 'Value' in statuslist:
        print "Error getting job status from DIRAC!"
    
    for i in joblist:
        # get status from Dirac
        if not i[2] in statuslist['Value']:
            status = "Missing"
        status = statuslist['Value'][i[2]]['Status']
        # if it failed, add it to the failed list
        if status == "Failed":
            failedlist.append(i);
        # if it completed, add it to the completed list
        elif status == "Done":
            completedlist.append(i)
            sitelist.append(statuslist['Value'][i[2]]['Site'])
        elif status == "Waiting":
            submittedjobs = submittedjobs + 1
        elif status == "Running":
            runningjobs = runningjobs + 1
        else:
            print "Job", i[2], "has status", status
            otherjobs = otherjobs + 1

    # print out status stats
    print "Current job distribution:"
    print "  ", submittedjobs, "submitted"
    print "  ", runningjobs, "running"
    print "  ", len(failedlist), "failed"
    print "  ", len(completedlist), "completed"
    print "  ", otherjobs, "other"

    # process failed list
    for i in failedlist:
        print "Job", i, "failed, resubmitting..."
        # remove this job from the main list now
        removeJobFromList(joblist, i[2])

        # resubmit it
        success = False
        while not success:
            try:
                submitImsimJob(dirac, joblist, i[0], i[1])
                success = True
            except:
                print "Resubmission failed, trying again..."
                pass


    # process completed list
    for j in range(0, len(completedlist)):
        i = completedlist[j]
        site = sitelist[j]
        print "Job", i, "completed at site", site, ", adding to completed list..."
        # remove this job from the main list now
        removeJobFromList(joblist, i[2])

        # append it to the completed list file
        compfile = open(completedlistfile, 'a')
        compfile.write(i[0] + " " + str(i[1]) + " " + str(i[2]) + "\n")
        compfile.close()

        # append site to the site list file
        compfile = open(sitelistfile, 'a')
        compfile.write(str(i[2]) + " " + site + "\n")
        compfile.close()

    time.sleep(5)

    # start new jobs if desired
    if len(joblist) < DESIRED_JOB_COUNT and visitpos < len(visits):
        # start next batch
        submitBatch(dirac, joblist, visits[visitpos]);
        visitpos = visitpos + 1

        # save visit position
        visitposfile = open(visitposition, 'w')
        visitposfile.write(str(visitpos) + "\n")
        visitposfile.close()

    # write job list back to file at end of loop
    writeJobList(joblistfile, joblist)

print "Exited main loop"

