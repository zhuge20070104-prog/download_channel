#!/usr/bin/env python3
"""Empty an S3 bucket including all object versions and delete markers.

Used by `make destroy` / `make empty-s3-buckets` before `terraform destroy`,
because the project buckets have versioning enabled and force_destroy is unset,
so `terraform destroy` fails with BucketNotEmpty unless versions are wiped.
"""
import sys

import boto3


def empty_bucket(bucket: str) -> None:
    s3 = boto3.client("s3")
    total = 0
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        objects = [
            {"Key": v["Key"], "VersionId": v["VersionId"]}
            for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        if not objects:
            continue
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
        total += len(objects)
    print(f"  deleted {total} objects/versions from {bucket}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: empty_s3_bucket.py <bucket-name>", file=sys.stderr)
        sys.exit(2)
    empty_bucket(sys.argv[1])
