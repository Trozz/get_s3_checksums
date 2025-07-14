#!/usr/bin/env python
"""
Get checksums of objects in Amazon S3.

This script creates a spreadsheet with the checksums of all the objects
within a given S3 prefix.  Prints the name of the finished spreadsheet.

Usage:
    get_s3_checksums.py <S3_PREFIX> [--checksums=<CHECKSUMS>] [--concurrency=<CONCURRENCY>] [--force]
    get_s3_checksums.py (-h | --help)

Options:
    -h --help                    Show this screen.
    --checksums=<CHECKSUMS>      Comma-separated list of checksums to fetch.
                                 [default: md5,sha1,sha256,sha512]
    --concurrency=<CONCURRENCY>  Max number of objects to fetch from S3 at once.
                                 [default: 5]
    --force                      Force recalculation even if tags already exist.
"""

import csv
import hashlib
import secrets
import sys
import urllib.parse

import boto3
import docopt
from botocore.exceptions import ClientError

from concurrently import concurrently


def list_s3_objects(sess, *, bucket, prefix):
    client = sess.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for s3_obj in page.get("Contents", []):
            yield {"bucket": bucket, "key": s3_obj["Key"]}


def check_existing_tags(s3, bucket, key, required_checksums):
    """Check if object already has all required checksum tags."""
    try:
        response = s3.get_object_tagging(Bucket=bucket, Key=key)
        existing_tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        
        # Check if all required checksums exist as tags
        for checksum in required_checksums:
            if checksum not in existing_tags:
                return False, existing_tags
        
        return True, existing_tags
    except ClientError:
        return False, {}


def get_s3_object_checksums(sess, *, bucket, key, checksums, force=False):
    s3 = sess.client("s3")
    
    # Check if we should skip this object
    if not force:
        has_all_tags, existing_tags = check_existing_tags(s3, bucket, key, checksums)
        if has_all_tags:
            # Object already has all required tags, return them without recalculating
            try:
                s3_obj = s3.head_object(Bucket=bucket, Key=key)
                result = {
                    "bucket": bucket,
                    "key": key,
                    "size": s3_obj["ContentLength"],
                    "ETag": s3_obj["ETag"],
                    "VersionId": s3_obj.get("VersionId", ""),
                    "last_modified": s3_obj["LastModified"].isoformat(),
                    "skipped": True  # Mark that we skipped calculation
                }
                
                # Add existing checksums from tags
                for name in checksums:
                    result[f"checksum.{name}"] = existing_tags.get(name, "")
                
                return result
            except ClientError:
                pass  # Fall through to calculate checksums
    
    # Calculate checksums
    hashes = {name: hashlib.new(name) for name in checksums}
    
    s3_obj = s3.get_object(Bucket=bucket, Key=key)

    while chunk := s3_obj["Body"].read(8192):
        for hv in hashes.values():
            hv.update(chunk)

    result = {
        "bucket": bucket,
        "key": key,
        "size": s3_obj["ContentLength"],
        "ETag": s3_obj["ETag"],
        "VersionId": s3_obj.get("VersionId", ""),
        "last_modified": s3_obj["LastModified"].isoformat(),
        "skipped": False
    }

    # Calculate checksums and prepare tags
    tags = {}
    for name, hv in hashes.items():
        checksum_value = hv.hexdigest()
        result[f"checksum.{name}"] = checksum_value
        tags[name] = checksum_value

    # Set tags on the S3 object
    try:
        tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]
        s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": tag_set}
        )
        if s3_obj.get("VersionId"):
            # If versioning is enabled, tag the specific version
            s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                VersionId=s3_obj["VersionId"],
                Tagging={"TagSet": tag_set}
            )
    except Exception as e:
        # Log the error but don't fail the checksum calculation
        print(f"Warning: Failed to set tags for {bucket}/{key}: {e}", file=sys.stderr)

    return result


def main():
    args = docopt.docopt(__doc__)

    checksums = args["--checksums"].split(",")
    max_concurrency = int(args["--concurrency"])
    force = args["--force"]

    for h in checksums:
        if h not in hashlib.algorithms_available:
            sys.exit(f"Unavailable/unrecognised checksum algorithm: {h!r}")

    s3_prefix = args["<S3_PREFIX>"]
    bucket = urllib.parse.urlparse(s3_prefix).netloc
    prefix = urllib.parse.urlparse(s3_prefix).path.lstrip("/")

    s3_slug = s3_prefix.replace("s3://", "").replace("/", "_")
    random_suffix = secrets.token_hex(3)
    out_path = f"checksums.{s3_slug}.{random_suffix}.csv"

    sess = boto3.Session()

    fieldnames = ["bucket", "key", "size", "ETag", "VersionId", "last_modified"] + [
        f"checksum.{name}" for name in checksums
    ]

    total_objects = 0
    total_skipped = 0
    with open(out_path, "w") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for _, output in concurrently(
            lambda s3_obj: get_s3_object_checksums(sess, **s3_obj, checksums=checksums, force=force),
            list_s3_objects(sess, bucket=bucket, prefix=prefix),
            max_concurrency=max_concurrency
        ):
            # Don't write the 'skipped' field to CSV
            skipped = output.pop('skipped', False)
            writer.writerow(output)
            total_objects += 1
            
            if skipped:
                total_skipped += 1
            
            if total_objects % 100 == 0:
                print(f"Processed {total_objects} objects ({total_skipped} skipped)...", file=sys.stderr)

    print(f"Total objects processed: {total_objects} ({total_skipped} skipped)", file=sys.stderr)
    print(out_path)


if __name__ == "__main__":
    main()
