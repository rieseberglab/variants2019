#!/usr/bin/env python3
import sys
import argparse
import os.path
import json
from collections import OrderedDict

topdir = os.path.dirname(__file__) + "/.."
sample_sources = os.path.join(topdir, "sample-info", "samples", "sequence_sources_oct_2019.tsv")


def sources_db():
    by_sample = OrderedDict()

    is_header = True
    with open(sample_sources, "r") as infd:
        for lineno, line in enumerate(infd):
            line = line.strip()
            if not line or line.startswith("#"): continue

            if is_header:
                is_header = False
                toks = line.split("\t")
                assert toks[0] == "SAMPLENAME"
                assert toks[4] == "SRC_RUN"
                assert toks[5] == "SRC_URL"
                assert toks[6] == "EXTRA"
                continue

            # SAMPLENAME      SRC_TYPE        SRC_PROJECT     SRC_SAMPLENAME  SRC_RUN SRC_URL EXTRA
            toks = line.split("\t")
            samplename, src_type, src_project, src_samplename, src_run, src_url = toks[0:6]
            extra = toks[6:] if len(toks) > 6 else []

            item = {
                'samplename': samplename,
                'runid': src_run,
                'r1_url': None,
                'r2_url': None,
                'digests': {}
            }

            for tag in extra:
                if ":" not in tag:
                    continue
                algo, digest = tag.split(":", maxsplit=1)
                item['digests'][algo] = digest

            runs = by_sample.setdefault(samplename, OrderedDict())

            if src_run not in runs:
                runs[src_run] = item

            run = runs[src_run]
            if src_url.endswith("_R1.fastq.gz"):
                if run['r1_url']:
                    raise Exception("duplicate %s run %s R1" % (samplename, src_run))
                run['r1_url'] = src_url
            elif src_url.endswith("_R2.fastq.gz"):
                if run['r2_url']:
                    raise Exception("duplicate %s run %s R2" % (samplename, src_run))
                run['r2_url'] = src_url
            elif src_url.endswith(".sra"):
                if run['r1_url'] or run['r2_url']:
                    raise Exception("duplicate %s run %s sra" % (samplename, src_run))
                run['r1_url'] = src_url
            else:
                raise Exception("unrecognized url " + src_url)

    return by_sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", metavar="SOURCE", type=str, default="-",
                        help="path to samplenames")

    args = parser.parse_args()

    infile = args.source
    if infile == "-":
        infd = sys.stdin
    else:
        infd = open(args.source, "r")

    sources = sources_db()
    outfd = sys.stdout

    for line in infd:
        line = line.strip()
        if not line or line.startswith("#"): continue
        toks = line.split()
        samplename = toks[0]
        rest = toks[1:]
        # output sample_name:SAMPLENAME species:SPECIES run:RUN_ID url:URL md5:DIGEST
        runs = sources[samplename]
        for runid, run in runs.items():
            entry = {
                'sample_name': samplename,
                'species': rest[0],
                'run': runid,
                'r1_url': run['r1_url'],
                'r2_url': run['r2_url'],
                'md5': run['digests']['md5']
            }
            outfd.write(json.dumps(entry, sort_keys=True)+"\n")

if __name__ == "__main__":
    main()
    sys.exit(0)

