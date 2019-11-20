#!/usr/bin/env python3
# -*- charset: utf-8; -*-

"""
Perform variant calling on input samples
"""

# framework
import bunnies
import bunnies.runtime
import os
import os.path
import logging
import sys
import argparse
import json
from collections import namedtuple

# experiment specific
from . import InputFile, Align, Merge, setup_logging

log = logging.getLogger(__package__)

HERE = os.path.dirname(__file__)
TOPDIR = os.path.join(HERE, "..")
bunnies.runtime.add_user_deps(TOPDIR, "variants", excludes=("__pycache__"))
bunnies.runtime.add_user_deps(TOPDIR, "scripts")
bunnies.runtime.add_user_hook("import variants")
bunnies.runtime.add_user_hook("variants.setup_logging()")

Reference = namedtuple("Reference", ["name", "ref", "ref_idx"])


def get_reference(shortname):
    shortname = shortname.lower()
    if shortname in get_reference.cache:
        return get_reference.cache[shortname]

    if shortname in ("ha412",):
        ref     = InputFile("s3://rieseberg-references/HA412/genome/Ha412HOv2.0-20181130.fasta",
                            desc="Ha412HO genome reference (.fasta)")
        ref_idx = InputFile("s3://rieseberg-references/HA412/genome/Ha412HOv2.0-20181130.fasta.fai",
                            desc="Ha412HO genome reference index")

    elif shortname in ("xrqv2",):
        ref = InputFile("s3://rieseberg-references/HanXRQ2.0-20180814/annotated/HanXRQr2.0-SUNRISE-2.1.genome.fasta",
                        desc="HanXRQv2 genome reference (.fasta)")
        ref_idx = InputFile("s3://rieseberg-references/HanXRQ2.0-20180814/annotated/HanXRQr2.0-SUNRISE-2.1.genome.fasta.fai",
                        desc="HanXRQv2 genome reference index")

    elif shortname in ("psc8",):
        ref = InputFile("s3://rieseberg-references/HanPSC8r1.0-20181105/HanPSC8_genome.fasta",
                        desc="HanPSC8v1 genome reference (.fasta)")
        ref_idx = InputFile("s3://rieseberg-references/HanPSC8r1.0-20181105/HanPSC8_genome.fasta.fai",
                        desc="HanPSC8v1 genome reference index")
    else:
        raise Exception("unrecognized reference name: " + shortname)

    get_reference.cache[shortname] = Reference(shortname, ref, ref_idx)
    return get_reference.cache[shortname]
get_reference.cache = {}


def main():
    setup_logging(logging.INFO)
    bunnies.setup_logging(logging.INFO)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", metavar="SAMPLESJSON", type=str, default="-",
                        help="input samples file in json format")
    args = parser.parse_args()

    infile = args.samples
    if infile == "-":
        infd = sys.stdin
    else:
        infd = open(args.samples, "r")

    runs = []
    Run = namedtuple("Run", ["sample_name", "r1", "r2", "species", "runid"])
    for line in infd:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        obj = json.loads(line)
        r1, r2 = obj['r1'], obj['r2']
        digest_keys = ('md5', 'sha1', 'sha256')
        r1_digests = {k:v for k,v in r1[1].items() if r1[1] and k in digest_keys} if r1 else None
        r2_digests = {k:v for k,v in r2[1].items() if r2[1] and k in digest_keys} if r2 else None
        runs.append(Run(
            sample_name=obj['sample_name'],
            r1=InputFile(obj['r1'][0], digests=r1_digests),
            r2=(InputFile(obj['r2'][0], digests=r2_digests) if r2 else None),
            species=obj['species'],
            runid=obj['runid']
        ))
    log.info("processing %d sequencing runs...", len(runs))

    targets = []
    references = {
        name: get_reference(name)
        for name in ("xrqv2", "psc8", "ha412")
    }

    all_merges = []
    all_bams = []
    for refname, ref in references.items():
        by_name = {}
        for run in runs:
            bam = Align(sample_name=run.sample_name,
                        r1=run.r1,
                        r2=run.r2,
                        ref=ref.ref,
                        ref_idx=ref.ref_idx,
                        lossy=False)
            all_bams.append(bam)
            by_name.setdefault(run.sample_name, []).append(bam)
        for sample_name in by_name:
            sample_bams = by_name[sample_name]
            # merge all the runs of that sample name in a single bam
            merged = Merge(sample_name, sample_bams)
            all_merges.append(merged)

    # - fixates software versions and parameters
    # - creates graph of dependencies
    log.info("building pipeline...")
    pipeline = bunnies.build_pipeline(all_merges[0:55])
    log.info("pipeline built...")

    # for build_node in pipeline.build_order():
    #     print(build_node.data, build_node.data.output_prefix())
    # return 0

    #
    # Create compute resources, tag the compute environment
    # entities with the name of the package
    #
    pipeline.build(os.path.basename("variants4"))

    def _shortname_of(s3_ref):
        for shortname, known_ref in references.items():
            if known_ref.ref is s3_ref:
                return shortname
        else:
            raise Exception("cannot find reference name for %s" % (str(s3_ref),))

    for target in pipeline.targets:
        merged = target.data
        refname = _shortname_of(merged.get_reference())
        print("\t".join([refname, merged.sample_name, merged.output_prefix()]))

def dostuff():
    # merge them by key
    merged_bam1 = Merge("ANN0830", [bam for bam in all_bams if bam.sample_name == "ANN0830"])
    merged_bam2 = Merge("ANN0832", [bam for bam in all_bams if bam.sample_name == "ANN0832"])

    all_merged = [merged_bam1, merged_bam2]

    pipeline = bunnies.build_pipeline(all_merged)

    # TODO - a URL where we can see details and progress in the browser (maybe via lambda+apigateway)
    # print(pipeline.dashboard_url())



if __name__ == "__main__":
    main()
    sys.exit(0)
