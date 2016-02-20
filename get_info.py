import os
from collections import defaultdict
import csv
import subprocess
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter

printing = False
files_open = []
file_reads = []
file_writes = []
files_deleted = []

def uniq(seq):
   # Not order preserving
   keys = {}
   for e in seq:
       keys[e] = 1
   return keys.keys()

# with

with open(os.getcwd() + "/procs.csv", "w") as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    csvwriter.writerow(['procname', 'files opened', 'average size', 'gluster files',
                  'afs files', 'cvmfs files', 'deleted files', 'gluster size',
                        'afs size', 'cvmfs size', 'scratch files', 'deleted size'])

for dirname, subdirs, pidfiles in os.walk(os.getcwd()):
    if not dirname:
        continue
 #   if "infos" in dirname.split("/")[-1]:

    if "procs" in dirname.split("/")[-1]:
        for pidfile in pidfiles:
            with open(dirname + "/" + pidfile) as currfile:
                gluster = 0
                glustersize = 0
                afs = 0
                afssize =0
                cvmfs = 0
                cvmfssize = 0
                sizes = 0
                i = 0
                deleted = 0
                deletedsize = 0
                scratch = 0
                scratchsize = 0
                del_not_tmp = 0
                tcp = 0
                proc_read = 0
                num_lines = 0
                proc_write = 0
                proc_io = 0

                csv_line = []

                sizedict = defaultdict(list)

                process = None
                pid = None

                try:
                    currfile = uniq(currfile.readlines())
                except Exception as e:
                    continue
                for line in currfile:
                    line = line.split()
                    if len(line) < 7:
                        continue
                    if not line[0].startswith("condor") and not line[0].startswith("squid") \
                            and not line[0].startswith("COMMAND") and not line[0].startswith("ptrac"):
                        if i is 0:
                            process = line[0]
                            pid = line[1]
                            csv_line.append(process)
                        if line[6].isnumeric() and len(line) > 8:
                            filesize = int(line[6])
                            if "/var/lib/condor/execute" in line[8]:
                                scratch += 1
                                scratchsize += filesize
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
                                deletedsize += int(line[6])
                                if "tmp" not in line[-1]:
                                    del_not_tmp += 1
                            if "TCP" in line[8]:
                                tcp += 1
                            sizes += int(line[6])
                            sizedict[line[1]].append(filesize)
                        i += 1
                if i is not 0:
                    filename = "/tmp" + dirname[-1] + ".txt"
                    with open(dirname + filename) as iotop_data:
                        for line in iotop_data:
                            if pid in line:
                                proc_read += float(line.split()[4])
                                proc_write += float(line.split()[6])
                                proc_io += float(line.split()[10])
                                num_lines += 1
                    sig_size = "%.3f" % ((sizes/i)/(1024*1024))
                    csv_line.append(i)
                    csv_line.append(str(sig_size))
                    files_open.append(i)
                    if num_lines and printing:
                        print(process, pid, "opened", i, "files with an average size of", sig_size, "MB")
                        print(" - READ average:", "%.2f" % (proc_read/num_lines), "KB/s")
                        print(" - WRITE average:", "%.2f" % (proc_write/num_lines), "KB/s")
                        print(" - TOTAL IO average:", "%.2f" % (proc_io/num_lines), "%")
                    if gluster:
                        gluster_sig_size = "%.3f" % ((glustersize/gluster)/(1024*1024))
                        if printing:
                            print("  - gluster files opened:", gluster, "with an average size of",
                              gluster_sig_size, "MB")
                    else:
                        csv_line.append('None')
                    if afs:
                        afs_sig_size = "%.3f" % ((afssize/afs)/(1024*1024))
                        csv_line.append(afs_sig_size)
                        if printing:
                            print("  - afs files opened:", afs, "with an average size of",
                             afs_sig_size , "MB")
                    else:
                        csv_line.append('None')
                    if cvmfs:
                        cvmfs_sig_size = "%.3f" % ((cvmfssize/cvmfs)/(1024*1024))
                        csv_line.append(cvmfs_sig_size)
                        if printing:
                            print("  - cvmfs files opened:", cvmfs, "with an average size of",
                              cvmfs_sig_size, "MB")
                    else:
                        csv_line.append('None')
                    if scratch:
                        scratch_sig_size = "%.3f" % ((scratchsize/scratch)/(1024*1024))
                        csv_line.append(scratch_sig_size)
                        if printing:
                            print("  - local files opened:", scratch, "with an average size of",
                              scratch_sig_size, "MB")
                    else:
                        csv_line.append('None')
                    if deleted:
                        del_sig_size = "%3.3f" % ((deletedsize/deleted)/(1024*1024))
                        csv_line.append(del_sig_size)
                        files_deleted.append(del_sig_size)
                        if printing:
                            print("  -", deleted, "files were deleted with an average size of",
                              del_sig_size, "MB")
                    else:
                        csv_line.append('None')

                   # csv_line.append("\n")
                    with open(os.getcwd() + "/procs.csv", "a") as csvfile:
                        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
                        csvwriter.writerow(csv_line)

bins = np.linspace(0, 10000, 50)
plt.hist(files_open, bins, alpha=0.5)
plt.xlabel("# files open")
plt.savefig("filesopen.png")
plt.hist(file_reads, bins, alpha=0.5)
plt.savefig("filereads.png")
plt.hist(file_writes, bins, alpha=0.5)
plt.savefig("filewrites.png")
#hist, bin_edges = np.histogram(files_deleted, bins=bins)
#plt.figure(num=None, figsize=(8, 6), dpi=200, facecolor='w', edgecolor='k')
#center = (bins[:-1] + bins[1:]) / 2
#plt.bar(center, hist, align='center')
#plt.savefig("filesdeleted.png")
                #
                # for pidkey in sizedict.keys():
                #     filelist = sizedict[pidkey]
                #     #for i in range(len(filelist)-1):
                #      #   filelist[i] = filelist[i]
                #     #hist, bins = np.histogram(filelist, bins=50)
                #    # width = 0.7 * (bins[1] - bins[0])
                #     #center = (bins[:-1] + bins[1:]) / 2
                #
                #     x, bins, p = plt.hist(filelist, log=True)
                #     #plt.bar(center, hist, align='center', width=width)
                #    # ax.set_yscale('log')
                #     #plt.tick_params(axis='y', which='minor')
                #    # ax.yaxis.set_minor_formatter(FormatStrFormatter("%.1f"))
                #    # plt.xticks(bins, np.logspace(0.1, 1.0, 50))
                #    # ax = plt.gca().set_yscale("log")
                #    # ax.set_ylim(1, 30)
                #     plt.xlabel('filesize (bytes)')
                #     plt.ylabel('# of files')
                #     plt.title('Files by size for ' + pidkey)
                #     plt.savefig(pidkey + ".png")




