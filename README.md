These are the scripts used to run ImSim for DC2 Run 2.1i on the UK grid.


Worker Node Script Descriptions
-------------------------------
runimsim2.1.sh is the script actually launched on the worker node when an ImSim job starts up. It unpacks the input instance catalogue file (which will have been automatically fetched from a storage element by this point) and runs ImSim with the correct arguments, then at the end of the job it packs up the generated FITS files into an archive named fits_<visit>_<index>.tar ready for upload to a storage element. It is also responsible for fetching, unpacking and setting up ImSim from a tar archive on a storage element, for compute nodes that do not have the required CVMFS repository mounted.

This script takes 5 arguments:

 1. The visit number. This is an 8 digit number and can be used to derive the name of the instance catalogue archive file.

 2. The name of the top level text file inside the instance catalogue archive.

 3. The zero-based index of the first sensor to run (0-188).

 4. The number of processes that ImSim should use, which is also the number of sensors that should be simulated. This is usually set to 4 for these jobs, except for the last job of a visit, for which it is set to 1.

 5. The index of this job within the visit (in the range 0-47).

Sometimes, if the instance catalogue does not contain any objects to be drawn onto any of the sensors, a job will produce no output. In this case the script creates a dummy output file. This is necessary because the grid middleware will generate an error if the output file does not exist, and this is not an error condition.


run_imsim_nersc.py is a copy of the same ImSim driver script used for DC2 runs at NERSC. It is invoked by runimsim2.1.sh after the input has been prepared. The line which sets the GalSim share directory has been commented out as it is incompatible with the grid runs, but everything else remains exactly the same as at NERSC.


Main Client Script Description
------------------------------
jobmanager.py is the top level Python script that manages the submission of ImSim jobs via the Dirac API. It monitors the currently running jobs and automatically submits more if the number in the system falls below a certain threshold. It also automatically resubmits jobs that fail, and keeps a record of the grid sites at which the jobs run. Each visit requires 48 jobs in order to fully process it, and within each visit the jobs are tracked by an index number (0-47).

jobmanager.py maintains its state in a set of text files. The paths to these files are set near the top of the script. The function of these files is as follows:

 joblist.txt - contains a list of all the jobs currently in the system. Each line represents one job and contains the visit number (always 8 digits), the index number within the visit, and the Dirac job ID, separated by spaces. Before starting a new run, this should be created as an empty file.
 
 jobscompleted.txt - contains a list in exactly the same format as joblist.txt, but this time for completed jobs rather than active jobs. Again this should be created as an empty file before starting a new run.
 
 visitlist.txt - contains a list of visit IDs to process. Each line contains one 8 digit visit number. This is only read by the script, never written to. Before starting a new run, the visit IDs to be simulated should be put in here.
 
 visitposition.txt - contains a single number which keeps track of how many visits have already been submitted. Before starting a new run, this file should be initialised with "0".
 
 jobsites.txt - contains a list of Dirac job IDs and the name of the site where each one ran. Should be created as an empty file before starting a new run.

When the script is interrupted by CTRL-C, it will catch the signal and ensure that its current state is written to disk before terminating.

This script does not need to be running all the time. The jobs already submitted will progress fine without it, but no new jobs will be submitted until the script is run again.


How To Use The Client Script
----------------------------
To get up and running with this script, you need to:

 1. Edit the file paths within jobmanager.py to point to the right directories.
 2. Create joblist.txt, jobscompleted.txt and jobsites.txt as empty files.
 3. Create visitlist.txt containing a list of all visit numbers to be processed.
 4. Create visitposition.txt containing the number 0.
 5. Run jobmanager.py!


Client-side Utilities
---------------------
If you ever need to cancel all of the current jobs and resubmit them (for example because a setting needs to be changed), you can cancel them using the cancel_jobs.py script. Then rename joblist.txt to resubmissions.txt and replace joblist.txt with an empty file before running jobmanager.py. It will resubmit all the jobs in resubmissions.txt. Make sure you remove this file before running jobmanager.py again or the jobs will be resubmitted a second time!


submit_visit.py can be used to submit a single visit, or a subset of a visit, to the grid for processing. Its first argument is the 8 digit visit number to submit. If any further arguments are present, they represent indices in the range 0-47, for jobs within the visit that should be submitted. If no further arguments are given, all 48 jobs are submitted. The job IDs are recorded in a file named 'visit_jobs_<visit>.txt'. This script does not integrate with the infrastructure used by jobmanager.py; instead, jobs submitted this way must be checked on manually.


Common Errors
-------------
ImSim fetches a file from a military server in the USA every time it executes. Sometimes this server fails to respond which causes ImSim to fail. This can be identified by the presence of a warning like this in the job's output:

  WARNING: failed to download http://maia.usno.navy.mil/ser7/finals2000A.all, using local IERS-B: <urlopen error [Errno -2] Name or service not known> [astropy.utils.iers.iers]

This failure is intermittent and the same job should complete successfully next time.


Sometimes jobs run out of memory, since the instance catalogues take varying amounts of memory to process. This can manifest itself as the job entering a "stalled" state within Dirac. There is usually no error message. Sometimes the same job will work fine on the second attempt because it gets sent to a worker node with more memory available. It's also possible to change the number of processors allocated and the number of sensors processed by a single job to reduce the memory pressure (for example, allocating 4 or 8 processors but only running 1 or 2 sensors). This currently has to be done by modifying the Python code that submits the jobs.


Containerisation
----------------
Everything described above is the old method for running ImSim, based on a manual installation of ImSim and its dependencies on CVMFS. It is also possible to run it in a container via Singularity.

Currently, the main submission script has not been updated to support containers yet, but there is a test script that supports them. This is submit_visit_container.py. It works just the same as submit_visit.py but runs ImSim in a Singularity container.

It depends on a few additional files that are used on the worker node:

 - launch_container.sh is the top level script run on the worker node (instead of runimsim2.1.sh). It unpacks the input, runs the container, then packs up any generated output data.
 - docker_run.sh is the top level script that runs inside the container. It sets up the environment for running ImSim, then changes to the right directory and invokes the ImSim driver script, which is the same as before.
 - parsl_imsim_configs is a configuration file passed to ImSim. It is the same one used for ImSim runs at NERSC and elsewhere.

The image for the container is stored in Singularity's "sandbox" format (with the files stored in a normal directory tree rather than an image file) in /cvmfs/gridpp.egi.eu/lsst/imsim_sandbox/.
