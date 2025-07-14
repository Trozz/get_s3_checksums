#!/usr/bin/env python
"""
Get checksums of objects in ALL Amazon S3 buckets in an account.

This script creates a spreadsheet with the checksums of all the objects
across all S3 buckets in your AWS account. You can filter buckets by
name pattern and limit object processing.

Usage:
    get_all_s3_checksums.py [--checksums=<CHECKSUMS>] [--concurrency=<CONCURRENCY>] [--bucket-filter=<FILTER>] [--max-objects=<MAX>] [--skip-empty] [--force]
    get_all_s3_checksums.py (-h | --help)

Options:
    -h --help                    Show this screen.
    --checksums=<CHECKSUMS>      Comma-separated list of checksums to fetch.
                                 [default: md5,sha1,sha256,sha512]
    --concurrency=<CONCURRENCY>  Max number of objects to fetch from S3 at once.
                                 [default: 5]
    --bucket-filter=<FILTER>     Only process buckets matching this pattern (supports wildcards).
    --max-objects=<MAX>          Maximum number of objects to process per bucket.
    --skip-empty                 Skip buckets with no objects.
    --force                      Force recalculation even if tags already exist.
"""

import csv
import hashlib
import secrets
import sys
import urllib.parse
import fnmatch
from datetime import datetime

import boto3
import docopt
from botocore.exceptions import ClientError

from concurrently import concurrently


def list_all_buckets(sess):
    """List all S3 buckets in the account."""
    s3 = sess.client('s3')
    try:
        response = s3.list_buckets()
        return [(bucket['Name'], bucket['CreationDate']) for bucket in response['Buckets']]
    except ClientError as e:
        print(f"Error listing buckets: {e}", file=sys.stderr)
        return []


def get_bucket_region(sess, bucket_name):
    """Get the region of a bucket."""
    s3 = sess.client('s3')
    try:
        response = s3.get_bucket_location(Bucket=bucket_name)
        region = response.get('LocationConstraint')
        # get_bucket_location returns None for us-east-1
        return region if region else 'us-east-1'
    except ClientError:
        return None


def list_s3_objects(sess, *, bucket, prefix="", max_objects=None):
    """List objects in an S3 bucket with optional limit."""
    region = get_bucket_region(sess, bucket)
    if not region:
        print(f"Warning: Could not determine region for bucket {bucket}, skipping", file=sys.stderr)
        return
    
    client = sess.client("s3", region_name=region)
    paginator = client.get_paginator("list_objects_v2")
    
    count = 0
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for s3_obj in page.get("Contents", []):
                yield {"bucket": bucket, "key": s3_obj["Key"], "region": region}
                count += 1
                if max_objects and count >= max_objects:
                    return
    except ClientError as e:
        print(f"Warning: Error listing objects in {bucket}: {e}", file=sys.stderr)


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


def get_s3_object_checksums(sess, *, bucket, key, region, checksums, force=False):
    """Get checksums for an S3 object and set tags."""
    s3 = sess.client("s3", region_name=region)
    
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
    
    try:
        s3_obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        print(f"Warning: Error getting object {bucket}/{key}: {e}", file=sys.stderr)
        return None

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


def count_bucket_objects(sess, bucket):
    """Count objects in a bucket (first 1000 only for efficiency)."""
    region = get_bucket_region(sess, bucket)
    if not region:
        return 0
    
    try:
        s3 = sess.client('s3', region_name=region)
        response = s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return response.get('KeyCount', 0)
    except ClientError:
        return 0


def main():
    args = docopt.docopt(__doc__)

    checksums = args["--checksums"].split(",")
    max_concurrency = int(args["--concurrency"])
    bucket_filter = args["--bucket-filter"]
    max_objects = int(args["--max-objects"]) if args["--max-objects"] else None
    skip_empty = args["--skip-empty"]
    force = args["--force"]

    for h in checksums:
        if h not in hashlib.algorithms_available:
            sys.exit(f"Unavailable/unrecognised checksum algorithm: {h!r}")

    sess = boto3.Session()
    
    # Get all buckets
    print("Listing all S3 buckets in the account...", file=sys.stderr)
    all_buckets = list_all_buckets(sess)
    
    if not all_buckets:
        sys.exit("No buckets found or unable to list buckets")
    
    # Filter buckets if requested
    if bucket_filter:
        filtered_buckets = [
            (name, created) for name, created in all_buckets 
            if fnmatch.fnmatch(name, bucket_filter)
        ]
        print(f"Found {len(filtered_buckets)} buckets matching filter '{bucket_filter}'", file=sys.stderr)
    else:
        filtered_buckets = all_buckets
        print(f"Found {len(filtered_buckets)} buckets total", file=sys.stderr)
    
    if not filtered_buckets:
        sys.exit("No buckets match the specified filter")
    
    # Check for empty buckets if requested
    if skip_empty:
        non_empty_buckets = []
        for bucket_name, created in filtered_buckets:
            if count_bucket_objects(sess, bucket_name) > 0:
                non_empty_buckets.append((bucket_name, created))
            else:
                print(f"Skipping empty bucket: {bucket_name}", file=sys.stderr)
        filtered_buckets = non_empty_buckets
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    random_suffix = secrets.token_hex(3)
    out_path = f"checksums.all_buckets.{timestamp}.{random_suffix}.csv"

    fieldnames = ["bucket", "key", "size", "ETag", "VersionId", "last_modified"] + [
        f"checksum.{name}" for name in checksums
    ]

    total_objects = 0
    total_skipped = 0
    with open(out_path, "w") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for bucket_name, created_date in filtered_buckets:
            print(f"\nProcessing bucket: {bucket_name} (created: {created_date.strftime('%Y-%m-%d')})", file=sys.stderr)
            
            bucket_objects = 0
            bucket_skipped = 0
            try:
                for _, output in concurrently(
                    lambda s3_obj: get_s3_object_checksums(sess, **s3_obj, checksums=checksums, force=force),
                    list_s3_objects(sess, bucket=bucket_name, max_objects=max_objects),
                    max_concurrency=max_concurrency
                ):
                    if output:  # Skip None results from errors
                        # Don't write the 'skipped' field to CSV
                        skipped = output.pop('skipped', False)
                        writer.writerow(output)
                        bucket_objects += 1
                        total_objects += 1
                        
                        if skipped:
                            bucket_skipped += 1
                            total_skipped += 1
                        
                        if bucket_objects % 100 == 0:
                            print(f"  Processed {bucket_objects} objects ({bucket_skipped} skipped)...", file=sys.stderr)
                
                print(f"  Completed: {bucket_objects} objects ({bucket_skipped} skipped)", file=sys.stderr)
                
            except Exception as e:
                print(f"  Error processing bucket {bucket_name}: {e}", file=sys.stderr)

    print(f"\nTotal objects processed: {total_objects} ({total_skipped} skipped)", file=sys.stderr)
    print(out_path)


if __name__ == "__main__":
    main()