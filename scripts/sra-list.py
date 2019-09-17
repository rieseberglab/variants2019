#!/usr/bin/env python3

"""
input is:
SAMPLENAME      SRC_TYPE        SRC_PROJECT     SRC_SAMPLENAME  SRC_RUN SRC_URL EXTRA
"""

import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", metavar="SOURCE", type=str, default="-",
                        help="path to index file (.fai), or - for stdin.")
    args = parser.parse_args()

    infile = args.source
    if infile == "-":
        infd = sys.stdin
    else:
        infd = open(args.source, "r")

    outfd = sys.stdout
    entries = []

    for line in infd:
        line = line.strip()
        if not line or line.startswith("#"): continue
        #SAMPLENAME      SRC_TYPE        SRC_PROJECT     SRC_SAMPLENAME  SRC_RUN SRC_URL EXTRA
        toks = line.split("\t")
        samplename, src_type, src_project, src_samplename, src_run, src_url = toks[0:6]
        extra = toks[6:] if len(toks) > 6 else []

        if src_type == "SRA":
            outfd.write("%s\t%s\t%s\n" % (src_project, samplename, src_run))

if __name__ == "__main__":
    main()
    sys.exit(0)
