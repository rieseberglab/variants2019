#!/usr/bin/env python3
import sys
import argparse
import os.path
import json
from collections import OrderedDict

topdir = os.path.dirname(__file__) + "/.."
sample_sources = os.path.join(topdir, "sample-info", "samples", "sequence_sources_apr_2020.tsv")


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
                'r1': None,
                'r2': None,
            }

            digests = {}
            for tag in extra:
                if ":" not in tag:
                    continue
                algo, digest = tag.split(":", maxsplit=1)
                digests[algo] = digest

            runs = by_sample.setdefault(samplename, OrderedDict())

            if src_run not in runs:
                runs[src_run] = item

            run = runs[src_run]
            if src_url.endswith("_R1.fastq.gz"):
                if run['r1']:
                    raise Exception("duplicate %s run %s R1" % (samplename, src_run))
                run['r1'] = (src_url, digests)
            elif src_url.endswith("_R2.fastq.gz"):
                if run['r2']:
                    raise Exception("duplicate %s run %s R2" % (samplename, src_run))
                run['r2'] = (src_url, digests)
            elif src_url.endswith(".sra"):
                if run['r1'] or run['r2']:
                    raise Exception("duplicate %s run %s sra" % (samplename, src_run))
                run['r1'] = (src_url, digests)
            else:
                raise Exception("unrecognized url " + src_url)

    return by_sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", metavar="SOURCE", type=str, default="-",
                        help="path to samplenames")

    parser.add_argument("--species", metavar="SPECIES", type=str, default=None)

    args = parser.parse_args()

    infile = args.source
    if infile == "-":
        infd = sys.stdin
    else:
        infd = open(args.source, "r")

    sources = sources_db()
    outfd = sys.stdout

    sample_errors = {}
    seen_names = {}
    for lineno, line in enumerate(infd):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        toks = line.split()
        samplename = toks[0]
        rest = toks[1:]

        if samplename in seen_names:
            sys.stderr.write("error on line %s: duplicate sample name %s\n" % (lineno+1, samplename))
            sample_errors[samplename] = True
            continue

        if len(rest) < 1 and not args.species:
            sys.stderr.write("error on line %s: sample %s doesn't specify a species and no default specified.\n" % (
                lineno+1, samplename))
            sample_errors[samplename] = True
            continue

        seen_names[samplename] = lineno + 1

        # output sample_name:SAMPLENAME species:SPECIES run:RUN_ID url:URL md5:DIGEST
        try:
            runs = sources[samplename]
        except KeyError:
            sys.stderr.write("error on line %s: name %s not found in sequence listing\n" % (lineno+1, samplename))
            sample_errors[samplename] = True
            continue

        for runid, run in runs.items():
            entry = {
                'sample_name': samplename,
                'species': rest[0] if args.species is None else args.species,
                'runid': runid,
                'r1': run['r1'],
                'r2': run['r2']
            }
            outfd.write(json.dumps(entry, sort_keys=True)+"\n")

    if sample_errors:
        sys.stderr.write("%d samples generated errors.\n" % (len(sample_errors),))

    if sample_errors:
        return min(1, len(sample_errors) % 256)
    return 0


if __name__ == "__main__":
    sys.exit(main())
