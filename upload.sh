#!/usr/bin/env python
#
# Script to register files uploaded via GridFTP in the Dirac File Catalogue.
# Input is a list of filenames and sizes as produced by upload script.
#

import sys
import uuid

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

SE = 'UKI-LT2-IC-HEP-disk'
PFNBASE = 'srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/lsst/user/j/james.perry/instcats/2.1i/'
LFNBASE = '/lsst/user/j/james.perry/instcats/2.1i/'

if len(sys.argv) != 2:
    print("Usage: register.py <file produced by upload script>")
    sys.exit(1)

f = open(sys.argv[1], 'r')
lines = f.readlines()
f.close()

fc = FileCatalog()

for line in lines:
    line = line.strip()
    bits = line.split(' ')
    filename = bits[0]
    if bits[1] == "ERROR!":
        print("Skipping file", filename, "due to upload error")
    else:
        size = int(bits[1])
        print("Registering file", filename, "with size", size)
        lfn = LFNBASE + filename
        
        infoDict = {}
        infoDict['PFN'] = PFNBASE + filename
        infoDict['Size'] = size
        infoDict['SE'] = SE
        infoDict['Checksum'] = ''
        infoDict['GUID'] = str(uuid.uuid4())

        fileDict = {}
        fileDict[lfn] = infoDict
        
        result = fc.addFile(fileDict)
        print("Result:", result)
        
