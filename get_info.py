import os
from collections import defaultdict
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter


def uniq(seq):
   # Not order preserving
   keys = {}
   for e in seq:
       keys[e] = 1
   return keys.keys()

for pidfile in os.listdir(os.getcwd() + "/procs/"):
    with open(os.getcwd() + "/procs/" + pidfile) as currfile:
        gluster = 0
        glustersize = 0
        afs = 0
        afssize =0
        cvmfs = 0
        cvmfssize = 0
        sizes = 0
        i = 0
        deleted = 0
        sizedict = defaultdict(list)
        process = None
        currfile = uniq(currfile.readlines())

        for line in currfile:
            line = line.split()
            if not line[0].startswith("condor") and not line[0].startswith("squid") and not line[0].startswith("COMMAND") and not line[0].startswith("ptrac"):
                if i is 0:
                    process = line[0]
                if line[6].isnumeric() and len(line) > 8:
                    filesize = int(line[6])
                    if "gluster" in line[8]:
                        gluster += 1
                        glustersize += filesize
                    if "afs" in line[8]:
                        afs += 1
                        afssize += filesize
                    if "cvmfs" in line[8]:
                        cvmfs += 1
                        cvmfssize += filesize
                    if "deleted" in line[-1]:
                        deleted += 1
                    sizes += int(line[6])
                    sizedict[line[1]].append(filesize)
                i += 1
        if i is not 0:
            print(process, "opened", i, "files with an average size of", "%.3f" % ((sizes/i)/(1024*1024)), "MB")
            if gluster:
                print("  - gluster files opened:", gluster, "with an average size of", "%.3f" % ((glustersize/gluster)/(1024*1024)), "MB")
            if afs:
                print("  - afs files opened:", afs, "with an average size of", "%.3f" % ((afssize/afs)/(1024*1024)), "MB")
            if cvmfs:
                print("  - cvmfs files opened:", cvmfs, "with an average size of", "%.3f" % ((cvmfssize/cvmfs)/(1024*1024)), "MB")
            if deleted:
                print("  -", deleted, "files were deleted")

        for pidkey in sizedict.keys():
            filelist = sizedict[pidkey]
            #for i in range(len(filelist)-1):
             #   filelist[i] = filelist[i]
            #hist, bins = np.histogram(filelist, bins=50)
           # width = 0.7 * (bins[1] - bins[0])
            #center = (bins[:-1] + bins[1:]) / 2

            x, bins, p = plt.hist(filelist, log=True)
            #plt.bar(center, hist, align='center', width=width)
           # ax.set_yscale('log')
            #plt.tick_params(axis='y', which='minor')
           # ax.yaxis.set_minor_formatter(FormatStrFormatter("%.1f"))
           # plt.xticks(bins, np.logspace(0.1, 1.0, 50))
            ax = plt.gca().set_yscale("log")
            ax.set_ylim(1, 30)
            plt.xlabel('filesize (bytes)')
            plt.ylabel('# of files')
            plt.title('Files by size for ' + pidkey)
            plt.savefig(pidkey + ".png")

