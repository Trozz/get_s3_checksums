#!/usr/bin/env python
"""
Get checksums of objects in ALL Amazon S3 buckets in an account.

This script creates a spreadsheet with the checksums of all the objects
across all S3 buckets in your AWS account. You can filter buckets by
name pattern and limit object processing.

Usage:
    get_all_s3_checksums.py [--checksums=<CHECKSUMS>] [--concurrency=<CONCURRENCY>] [--bucket-filter=<FILTER>] [--max-objects=<MAX>] [--skip-empty] [--force] [--parallel-buckets=<PARALLEL>]
    get_all_s3_checksums.py (-h | --help)

Options:
    -h --help                    Show this screen.
    --checksums=<CHECKSUMS>      Comma-separated list of checksums to fetch.
                                 [default: md5,sha1,sha256,sha512]
    --concurrency=<CONCURRENCY>  Max number of objects to fetch from S3 at once per bucket.
                                 [default: 5]
    --bucket-filter=<FILTER>     Only process buckets matching this pattern (supports wildcards).
    --max-objects=<MAX>          Maximum number of objects to process per bucket.
    --skip-empty                 Skip buckets with no objects.
    --force                      Force recalculation even if tags already exist.
    --parallel-buckets=<PARALLEL> Number of buckets to process in parallel.
                                 [default: 1]
"""

import csv
import hashlib
import secrets
import sys
import os
import tempfile
import shutil
import urllib.parse
import fnmatch
import time
import threading
from datetime import datetime
from collections import defaultdict

import boto3
import docopt
from botocore.exceptions import ClientError
from tqdm import tqdm

from concurrently import concurrently


class PerformanceTracker:
    """Track performance metrics across all bucket processing."""
    def __init__(self, total_buckets):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.total_buckets = total_buckets
        self.completed_buckets = 0
        self.bucket_metrics = defaultdict(lambda: {
            'objects': 0, 'skipped': 0, 'bytes': 0, 'start_time': None, 'end_time': None
        })
        self.total_objects = 0
        self.total_skipped = 0
        self.total_bytes = 0
    
    def start_bucket(self, bucket_name):
        with self.lock:
            self.bucket_metrics[bucket_name]['start_time'] = time.time()
    
    def update_bucket(self, bucket_name, objects=0, skipped=0, bytes_processed=0):
        with self.lock:
            self.bucket_metrics[bucket_name]['objects'] += objects
            self.bucket_metrics[bucket_name]['skipped'] += skipped
            self.bucket_metrics[bucket_name]['bytes'] += bytes_processed
            self.total_objects += objects
            self.total_skipped += skipped
            self.total_bytes += bytes_processed
    
    def complete_bucket(self, bucket_name):
        with self.lock:
            self.bucket_metrics[bucket_name]['end_time'] = time.time()
            self.completed_buckets += 1
    
    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            objects_per_sec = self.total_objects / elapsed if elapsed > 0 else 0
            mb_per_sec = (self.total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            
            # Estimate remaining time
            if self.completed_buckets > 0:
                avg_time_per_bucket = elapsed / self.completed_buckets
                remaining_buckets = self.total_buckets - self.completed_buckets
                estimated_remaining = avg_time_per_bucket * remaining_buckets
            else:
                estimated_remaining = 0
            
            return {
                'elapsed': elapsed,
                'objects_per_sec': objects_per_sec,
                'mb_per_sec': mb_per_sec,
                'total_objects': self.total_objects,
                'total_skipped': self.total_skipped,
                'total_mb': self.total_bytes / (1024 * 1024),
                'completed_buckets': self.completed_buckets,
                'total_buckets': self.total_buckets,
                'estimated_remaining': estimated_remaining
            }
    
    def print_progress(self):
        """Print current progress statistics."""
        stats = self.get_stats()
        elapsed_str = time.strftime('%H:%M:%S', time.gmtime(stats['elapsed']))
        remaining_str = time.strftime('%H:%M:%S', time.gmtime(stats['estimated_remaining']))
        
        print(f"\n=== Performance Metrics ===", file=sys.stderr)
        print(f"Elapsed: {elapsed_str} | Remaining: ~{remaining_str}", file=sys.stderr)
        print(f"Buckets: {stats['completed_buckets']}/{stats['total_buckets']} completed", file=sys.stderr)
        print(f"Objects: {stats['total_objects']} processed ({stats['total_skipped']} skipped)", file=sys.stderr)
        print(f"Speed: {stats['objects_per_sec']:.1f} objects/s | {stats['mb_per_sec']:.1f} MB/s", file=sys.stderr)
        print(f"Data: {stats['total_mb']:.1f} MB processed", file=sys.stderr)
        print("=" * 26, file=sys.stderr)


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


def process_bucket(bucket_info, sess, checksums, max_objects, max_concurrency, force, fieldnames, tracker, temp_dir, position, parallel_buckets=1):
    """Process a single bucket and write results to a temporary CSV file."""
    bucket_name, created_date = bucket_info
    
    tracker.start_bucket(bucket_name)
    
    # Create a temporary CSV file for this bucket
    temp_file = os.path.join(temp_dir, f"bucket_{bucket_name.replace('/', '_')}.csv")
    
    bucket_objects = 0
    bucket_skipped = 0
    bucket_bytes = 0
    
    # For remote systems, use simpler output
    print(f"[Bucket {position+1}] Starting: {bucket_name}", file=sys.stderr)
    
    # Create progress bar for this bucket
    # Disable multi-line progress bars on remote systems as they may not render properly
    use_progress_bar = os.environ.get('DISABLE_PROGRESS', '').lower() != 'true'
    
    if use_progress_bar:
        pbar = tqdm(
            total=None,  # Unknown total
            desc=f"{bucket_name[:30]:30}",  # Truncate long names
            unit="obj",
            position=position if parallel_buckets == 1 else None,  # Only use position for single bucket
            leave=True,
            ncols=100,  # Fixed width for remote terminals
            file=sys.stderr,  # Explicitly use stderr
            disable=position > 0 and parallel_buckets > 1,  # Disable extra bars in parallel mode
            bar_format="{desc} |{bar}| {n_fmt} [{elapsed}, {rate_fmt}]",
            postfix={'skipped': 0}
        )
    else:
        # Dummy progress bar that just counts
        class DummyProgressBar:
            def __init__(self):
                self.n = 0
                self.skipped = 0
            def update(self, n=1):
                self.n += n
            def set_postfix(self, postfix):
                self.skipped = postfix.get('skipped', 0)
            def set_description_str(self, desc):
                pass
            def close(self):
                pass
        pbar = DummyProgressBar()
    
    try:
        with open(temp_file, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            # Don't write header in temp files, we'll add it to the final file
            
            for _, output in concurrently(
                lambda s3_obj: get_s3_object_checksums(sess, **s3_obj, checksums=checksums, force=force),
                list_s3_objects(sess, bucket=bucket_name, max_objects=max_objects),
                max_concurrency=max_concurrency
            ):
                if output:  # Skip None results from errors
                    # Don't write the 'skipped' field to CSV
                    skipped = output.pop('skipped', False)
                    size = output.get('size', 0)
                    
                    writer.writerow(output)
                    
                    bucket_objects += 1
                    bucket_bytes += size
                    
                    if skipped:
                        bucket_skipped += 1
                        pbar.set_postfix({'skipped': bucket_skipped})
                        # Update description with skipped count
                        pbar.set_description(f"{bucket_name[:30]:30} (S:{bucket_skipped})")
                    
                    # Update progress bar
                    pbar.update(1)
                    
                    # Update tracker for overall stats
                    if bucket_objects % 10 == 0:
                        tracker.update_bucket(bucket_name, objects=10, 
                                            skipped=bucket_skipped if bucket_objects == 10 else 0, 
                                            bytes_processed=bucket_bytes)
                        bucket_bytes = 0  # Reset for next batch
                    
                    # Print status every 100 objects in parallel mode
                    if bucket_objects % 100 == 0 and parallel_buckets > 1:
                        print(f"[Bucket {position+1}] {bucket_name}: {bucket_objects} objects processed ({bucket_skipped} skipped)", file=sys.stderr)
            
            # Final update for remaining objects
            remaining_objects = bucket_objects % 10
            if remaining_objects > 0 or bucket_objects == 0:
                tracker.update_bucket(bucket_name, objects=remaining_objects, 
                                    skipped=bucket_skipped if bucket_objects < 10 else 0, 
                                    bytes_processed=bucket_bytes)
        
        # Mark as complete
        pbar.set_description_str(f"{bucket_name[:30]:30} ✓")
        pbar.close()
        print(f"[Bucket {position+1}] Completed: {bucket_name} - {bucket_objects} objects ({bucket_skipped} skipped)", file=sys.stderr)
        
    except Exception as e:
        pbar.set_description_str(f"{bucket_name[:30]:30} ✗")
        pbar.close()
        print(f"[Bucket {position+1}] Failed: {bucket_name} - Error: {str(e)[:100]}", file=sys.stderr)
        # Remove temp file on error
        if os.path.exists(temp_file):
            os.remove(temp_file)
        temp_file = None
    
    tracker.complete_bucket(bucket_name)
    return temp_file


def main():
    args = docopt.docopt(__doc__)

    checksums = args["--checksums"].split(",")
    max_concurrency = int(args["--concurrency"])
    bucket_filter = args["--bucket-filter"]
    max_objects = int(args["--max-objects"]) if args["--max-objects"] else None
    skip_empty = args["--skip-empty"]
    force = args["--force"]
    parallel_buckets = int(args["--parallel-buckets"])

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

    # Initialize performance tracker
    tracker = PerformanceTracker(len(filtered_buckets))
    
    # Create temporary directory for intermediate files
    temp_dir = tempfile.mkdtemp(prefix="s3_checksums_")
    temp_files = []
    
    try:
        # Print initial status
        print(f"\n🚀 Starting parallel processing with {parallel_buckets} concurrent bucket(s)", file=sys.stderr)
        print(f"📁 Processing {len(filtered_buckets)} buckets total\n", file=sys.stderr)
        
        # Create a counter for bucket positions
        position_counter = {'value': 0}
        position_lock = threading.Lock()
        
        # Process buckets in parallel
        def process_bucket_wrapper(bucket_info):
            with position_lock:
                position = position_counter['value']
                position_counter['value'] += 1
            
            return process_bucket(bucket_info, sess, checksums, max_objects, 
                                max_concurrency, force, fieldnames, tracker, temp_dir, position, parallel_buckets)
        
        # Collect temporary files from parallel processing
        for _, temp_file in concurrently(
            process_bucket_wrapper,
            filtered_buckets,
            max_concurrency=parallel_buckets
        ):
            if temp_file:
                temp_files.append(temp_file)
        
        # Add some spacing after progress bars
        print("\n" * (len(filtered_buckets) + 1), file=sys.stderr)
        
        # Combine all temporary CSV files into the final output
        print(f"📋 Combining {len(temp_files)} temporary files...", file=sys.stderr)
        
        with open(out_path, "w") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Read and combine all temp files
            for temp_file in temp_files:
                try:
                    with open(temp_file, 'r') as f:
                        reader = csv.DictReader(f, fieldnames=fieldnames)
                        for row in reader:
                            writer.writerow(row)
                except Exception as e:
                    print(f"Error reading temp file {temp_file}: {e}", file=sys.stderr)
        
        # Final statistics
        stats = tracker.get_stats()
        elapsed_str = time.strftime('%H:%M:%S', time.gmtime(stats['elapsed']))
        
        print(f"\n✅ Processing complete!", file=sys.stderr)
        print(f"⏱️  Total time: {elapsed_str}", file=sys.stderr)
        print(f"📊 Objects: {stats['total_objects']:,} processed ({stats['total_skipped']:,} skipped)", file=sys.stderr)
        print(f"📈 Speed: {stats['objects_per_sec']:.1f} objects/s | {stats['mb_per_sec']:.1f} MB/s", file=sys.stderr)
        print(f"💾 Data: {stats['total_mb']:.1f} MB processed", file=sys.stderr)
        print(f"\n📄 Output saved to: {out_path}", file=sys.stderr)
        print(out_path)
        
    finally:
        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Warning: Could not remove temp directory {temp_dir}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()