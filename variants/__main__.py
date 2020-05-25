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
from . import InputFile, Align, Merge, Genotype, setup_logging

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

    supported_references = ("xrqv2", "psc8", "ha412")

    parser = argparse.ArgumentParser(description=__doc__)

    # bunnies argumens
    parser.add_argument("--computeenv", metavar="ENVNAME", type=str, default="variants4",
                        help="assign this name to the compute environment resources")
    parser.add_argument("--maxattempt", metavar="N", type=int, default=2,
                        dest="max_attempt",
                        help="maximum number of times job is submitted before considering it failed (min 1)")
    parser.add_argument("--minattempt", metavar="M", type=int, default=1,
                        dest="min_attempt")
    parser.add_argument("--maxvcpus", metavar="VCPUS", type=int, default=1024,
                        dest="max_vcpus", help="the compute environment will scale to this upper limit for the number of VCPUs across all instances")

    # variant calling arguments
    parser.add_argument("samples", metavar="SAMPLESJSON", type=str, default="-",
                        help="input samples file in json format")
    parser.add_argument("--stage", metavar="STAGE", type=str, default="gvcf",
                        help="the stage of the pipeline to compute (bam, gvcf)",
                        choices=["bam", "gvcf"])
    parser.add_argument("--reference", metavar="REFNAME", choices=supported_references,
                        dest="references", action="append", default=[],
                        help="specify name of reference to consider. default is to do all of %s" %
                             (supported_references,))
    parser.add_argument("--starti", metavar="STARTI", type=int, default=0,
                        help="restrict pipeline to merges i>=starti (0based)")
    parser.add_argument("--endi",   metavar="ENDI",   type=int, default=9999999999,
                        help="restrict pipeline to merges i<=endi  (0based)")
    parser.add_argument("--dry-run", dest="dryrun", action="store_true", default=False,
                        help="don't build. just print the jobs that are ready.")

    args = parser.parse_args()

    infile = args.samples
    if infile == "-":
        infd = sys.stdin
    else:
        infd = open(args.samples, "r")

    args.references = set(args.references)
    if not args.references:
        args.references = set(supported_references)

    runs = []
    Run = namedtuple("Run", ["sample_name", "r1", "r2", "runid"])
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
            runid=obj['runid']
        ))
    log.info("processing %d sequencing runs...", len(runs))

    targets = []
    references = {
        name: get_reference(name)
        for name in args.references
    }

    log.info("running on selected references: %s", sorted([name for name in args.references]))

    all_merges = []
    all_bams = []
    all_gvcfs = []

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

            # call haplotypecaller
            gvcf = Genotype(sample_name, merged, hc_options=[
                "-G", "StandardAnnotation",
                "-G", "AS_StandardAnnotation",
                "-G", "StandardHCAnnotation"
            ])
            all_gvcfs.append(gvcf)

    # - fixates software versions and parameters
    # - creates graph of dependencies
    log.info("building pipeline...")

    def _clamp(i, minval, maxval):
        return min(max(minval, i), maxval)
    start_index = _clamp(args.starti, 0, len(all_gvcfs) - 1)
    end_index = _clamp(args.endi, 0, len(all_gvcfs) - 1)

    if args.stage == "gvcf":
        pipeline = bunnies.build_pipeline(all_gvcfs[start_index:end_index+1])
    elif args.stage == "bam":
        pipeline = bunnies.build_pipeline(all_merges[start_index:end_index+1])
    else:
        raise ValueError("unrecognized --stage value: %s" % (args.stage,))

    log.info("pipeline built...")

    #
    # Create compute resources, tag the compute environment
    # entities with the name of the package
    #
    if not args.dryrun:
        pipeline.build(args.computeenv,
                       min_attempt=args.min_attempt,
                       max_attempt=args.max_attempt,
                       max_vcpus=args.max_vcpus)
    else:
        log.info("dry run mode, skipping build.")

    def _shortname_of(s3_ref):
        for shortname, known_ref in references.items():
            if known_ref.ref is s3_ref:
                return shortname
        else:
            raise Exception("cannot find reference name for %s" % (str(s3_ref),))

    all_outputs = {}
    for target in pipeline.targets:
        transformed = target.data
        refname = _shortname_of(transformed.ref)
        all_outputs.setdefault(transformed.sample_name, {})[refname] = transformed

    headers = ["SAMPLENAME", "REFERENCE", "OUTPUTURL"]
    if args.dryrun:
        headers.append("COMPLETE")

    print("\t".join(headers))
    for sample_name in sorted(all_outputs.keys()):
        per_reference = all_outputs[sample_name]
        for refname in sorted(per_reference.keys()):
            transformed = per_reference[refname]
            output_url = transformed.exists()
            if not output_url:
                completed = False
                output_url = transformed.output_prefix()
            else:
                completed = True

            columns = [sample_name, refname, output_url]
            if args.dryrun:
                columns.append("true" if completed else "false")

            print("\t".join(columns))

if __name__ == "__main__":
    main()
    sys.exit(0)
