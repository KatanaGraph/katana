#!/usr/bin/python3

import argparse
import subprocess
import re

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', default="gpu.conf", type=str)
    args = parser.parse_args()

    proc = subprocess.Popen(["nvidia-smi", "-L"], stdout=subprocess.PIPE)
    outs, errs = proc.communicate()
    outs = outs.decode("utf-8")
    out = outs.splitlines()
    pattern = re.compile('GPU-[0-9a-f\-]*', re.I)

    f = open(args.o, 'w')
    for l in out:
        ret = re.search(pattern, l)
        uuid = ret.group(0)
        f.write('CUDA_VISIBLE_DEVICES={}\n'.format(uuid))
    f.close()
