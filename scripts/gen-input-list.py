#!/usr/bin/env python3

"""
Generates the list of input samples:

  SAMPLENAME RUNID URL DIGEST

"""

import sys
import argparse
import os.path

topdir = os.path.dirname(__file__) + "/.."

nanuq_hashes_file = os.path.join(topdir, "checksums", "nanuq-hashes.txt")
sra_hashes_file = os.path.join(topdir, "checksums", "sra-hashes.txt")

def nanuq_hash_db():
    with open(nanuq_hashes_file, "r") as hashes_fd:
        hashes = {}
        for line in hashes_fd:
            line = line.strip()
            if not line or line.startswith("#"): continue
            toks = line.split()
            proj, url = toks[0:2]
            basename = os.path.basename(url)
            if basename in hashes:
                raise Exception("duplicate filename %s" % (basename,))

            hashes[basename] = {
                'project': proj,
                'url': url,
                'digests': {}
            }
            for i in range(2, len(toks), 2):
                algo, digest = toks[i], toks[i+1]
                hashes[basename]['digests'][algo] = digest
        return hashes

def sra_hash_db():
    hashes = {}
    with open(sra_hashes_file, "r") as hashes_fd:
        for line in hashes_fd:
            line = line.strip()
            if not line or line.startswith("#"): continue
            toks = line.split()
            url, digest = toks[0:2]
            srafile = os.path.basename(url)
            srrno = os.path.splitext(srafile)[0]

            if srrno in hashes:
                raise Exception("duplicate sra %s" % (srrno,))

            proj = os.path.basename(os.path.dirname(url))
            assert proj.startswith("PRJ")

            hashes[srrno] = {
                'project': proj,
                'srr': srrno,
                'url': url,
                'digests': {digest.split(":")[0]: digest.split(":")[1]}
            }
        return hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", metavar="SOURCE", type=str, default="-",
                        help="path to index file (.fai), or - for stdin.")
    parser.add_argument("--extend", action='store_true', default=False)

    args = parser.parse_args()

    infile = args.source
    if infile == "-":
        infd = sys.stdin
    else:
        infd = open(args.source, "r")

    outfd = sys.stdout
    entries = []

    nanuq_hashes = nanuq_hash_db()
    sra_hashes = sra_hash_db()

    is_header = True
    for lineno, line in enumerate(infd):
        line = line.strip()
        if not line or line.startswith("#"):
            outfd.write(line + "\n")
            continue

        if is_header:
            is_header = False
            if args.extend:
                outfd.write(line + "\n")
            else:
                outfd.write("\t".join(["SAMPLENAME", "RUNID", "URL", "DIGEST"]) + "\n")
            continue

        # SAMPLENAME      SRC_TYPE        SRC_PROJECT     SRC_SAMPLENAME  SRC_RUN SRC_URL EXTRA
        toks = line.split("\t")
        samplename, src_type, src_project, src_samplename, src_run, src_url = toks[0:6]
        extra = toks[6:] if len(toks) > 6 else []

        item = {
            'samplename': samplename,
            'runid': src_run,
            'url': None,
            'md5': None
        }

        if src_type == "SRA":
            key = src_run
            if key not in sra_hashes:
                sys.stderr.write("Missing digest for SRR: %s\n" % (key,))
                continue

            if args.extend:
                toks += [algo + ":" + digest for algo, digest in sra_hashes[key]['digests'].items()]
                outfd.write("\t".join(toks) + "\n")
            else:
                item['url'] = sra_hashes[key]['url']
                item['md5'] = "md5:" + sra_hashes[key]['digests']['md5']
                outfd.write("%(samplename)s\t%(runid)s\t%(url)s\t%(md5)s\n" % item)

        elif src_type == "NANUQ":
            key = os.path.basename(src_url)

            if key.endswith(".md5"):
                # skip md5 urls
                continue

            if key not in nanuq_hashes:
                sys.stderr.write("Missing digest for file: %s\n" % (key,))
                continue

            if args.extend:
                toks += [algo + ":" + digest for algo, digest in nanuq_hashes[key]['digests'].items()]
                outfd.write("\t".join(toks) + "\n")
            else:
                item['url'] = nanuq_hashes[key]['url']
                item['md5'] = "md5:" + nanuq_hashes[key]['digests']['md5']
                outfd.write("%(samplename)s\t%(runid)s\t%(url)s\t%(md5)s\n" % item)
        else:
            sys.stderr.write("unknown type: %s\n" % (src_type,))

if __name__ == "__main__":
    main()
    sys.exit(0)
