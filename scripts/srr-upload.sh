#!/usr/bin/env bash

#
# Reads a list of SRR files
# - prefetches the SRA file
# - validates the SRA file
# - calculates its md5 checksum
# - writes the checksum to a log
# - uploads it to a bucket

DST_BUCKET=rieseberg-fastq

set -eo pipefail

mkdir -p sums
mkdir -p logs
sumsdir=$(readlink -f "sums")
logsdir=$(readlink -f "logs")

function process_sra
{
    local run="$1"
    local md5_out="$2"
    local dst_prefix="$3"
    local tmpcas
    local srafile

    echo "$$ processing SRR number: $run"
    tmpcas=$(mktemp -p cas.$run.XXXXXX)
    trap 'if [[ -d "$tmpcas" ]]; then rm -rf --one-file-system -- "$tmpcas"; fi' EXIT
    digest=$(cas -put "sra://$run" "$tmpcas")
    if [[ -z "$digest" ]]; then
	echo "$$ can't compute digest" >&2
	return 1
    fi
    srafile=$(cas -get "$digest")
    aws s3 cp --content-type application/octet-stream "$srafile" "$dst_prefix"
    echo "${dst_prefix}/$(basename "$srafile") $digest" > "${md5_out}".tmp
    mv "${md5_out}"{.tmp,}
    echo "$$ done processing SRR number: $run"
}

while read PRJ SAMPLE SRR; do
    if [[ ! -f "sums/$SRR.md5" ]]; then
	echo "set -eo pipefail; process_sra $SRR $sumsdir/$SRR.md5 s3://${DST_BUCKET}/sra/$PRJ/ 2>&1 | tee -a $logsdir/$SRR"
    fi
done | parallel --line-buffer -j 8 --halt soon,fail=1
