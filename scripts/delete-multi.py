#!/usr/bin/env python3

"""deletes multiple objects at once, from stdin"""

import sys
import boto3


def main():
    bucket = sys.argv[1].strip()

    def batch_reader(infd, maxlines):
        batch = []
        for line in infd:
            line = line.strip()
            if not line:
                continue
            batch.append(line)
            if len(batch) >= maxlines:
                yield batch
                batch = []
        if batch:
            yield batch
        batch = None

    client = boto3.client("s3")
    errors = 0
    num_deleted = 0
    for batch in batch_reader(sys.stdin, 1000):
        resp = client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [{
                    'Key': thekey
                } for thekey in batch]
            }
        )
        for error in resp.get('Errors', []):
            print("error %(Code)s with key %(Key)s %(VersionId)s: %(Message)s" % error)
            errors += 1
        print("deleted %d objects." % (len(resp.get('Deleted', [])),))
        num_deleted += len(resp.get('Deleted', []))

    print("total %d deleted. encountered %d errors." % (num_deleted, errors))
    if errors > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()

