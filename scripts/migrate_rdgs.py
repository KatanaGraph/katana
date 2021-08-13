#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
import sys

import boto3
from google.cloud import storage

s3_bucket_re_str = r"^(?:s3://)?(?:/)?(?P<bucket_name>[\-a-zA-Z0-9_]+)(?:\/)?(?P<prefix>[\/\-a-zA-Z0-9_\.]+)?(?:/)?$"
gs_bucket_re_str = r"^(?:gs://)?(?:/)?(?P<bucket_name>[\-a-zA-Z0-9_]+)(?:\/)?(?P<prefix>[\/\-a-zA-Z0-9_\.]+)?(?:/)?$"
meta_re_str = r"(?:(?P<prefix>.*)/)?meta_(?P<version>(?:\d+))$"
part_re_str = r"(?:(?P<prefix>.*)/)?meta_(?P<node_id>(?:\d+))_(?P<version>(?:\d+))$"
meta_re = re.compile(meta_re_str)  # r"meta_(\d+)")
part_re = re.compile(part_re_str)
s3_bucket_re = re.compile(s3_bucket_re_str)
gs_bucket_re = re.compile(gs_bucket_re_str)


def list_blobs(bucket_name, prefix):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    part_match_count = 0
    meta_match_count = 0
    for blob in blobs:
        try:
            file_name = blob.name.split("/")[-1]
            if part_re.match(file_name):
                part_match_count += 1
                yield file_name
                continue
            if meta_re.match(file_name):
                meta_match_count += 1
                yield file_name
                continue
        except IndexError:
            continue
    print(f"# found {meta_match_count} meta files and {part_match_count} partition files")


def get_s3_bucket_info(dst):
    match = s3_bucket_re.match(dst)
    try:
        bucket_name = match.group("bucket_name")
        prefix = match.group("prefix")
    except AttributeError:
        bucket_name = None
        prefix = None
    return bucket_name, prefix


def get_gs_bucket_info(dst):
    match = gs_bucket_re.match(dst)
    try:
        bucket_name = match.group("bucket_name")
        prefix = match.group("prefix")
    except AttributeError:
        bucket_name = None
        prefix = None
    return bucket_name, prefix


def get_s3_file_list(dst):
    bucket, prefix = get_s3_bucket_info(dst)
    from boto3 import client

    bucket_name = bucket

    s3_conn = client("s3")  # type: BaseClient  ## again assumes boto.cfg setup, assume AWS S3
    s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    if "Contents" not in s3_result:
        return []

    file_list = []
    for key in s3_result["Contents"]:
        file_name = key["Key"].split("/")[-1]
        file_list.append(file_name)

    while s3_result["IsTruncated"]:
        continuation_key = s3_result["NextContinuationToken"]
        s3_result = s3_conn.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/", ContinuationToken=continuation_key
        )
        for key in s3_result["Contents"]:
            file_name = key["Key"].split("/")[-1]
            file_list.append(file_name)
    meta_or_part_files = [x for x in file_list if meta_re.match(x) or part_re.match(x)]
    return meta_or_part_files


def get_gs_file_list(dst):
    bucket, prefix = get_gs_bucket_info(dst)
    if not bucket or not prefix or prefix[-1] != "/":
        print("#form must be: '--gs gs://${BUCKET_NAME}/${PATH_TO_RDG_DIR}/'")
        return []
    return list(list_blobs(bucket, prefix))


parser = argparse.ArgumentParser(description="Transform property graphs from old metadata format to new.")
parser.add_argument(
    "--no-dry-run",
    action="store_true",
    help="""If set, the operations will be performed on the target destinations.
    Otherwise, they will just output the corresponding CLI commands and the user can inspect to verify.""",
)
parser.add_argument("--s3", action="append")
parser.add_argument("--gs", action="append")
parser.add_argument("--fs", action="append")

args = parser.parse_args()


def gen_input_list(cat, l):
    try:
        return list(zip(len(l) * [cat], l))
    except TypeError:
        return []


def new_meta_name(path, dst):
    meta_match = meta_re.match(dst)
    try:
        version = int(meta_match.group("version"))
    except AttributeError:
        return None
    prefix = f"{path}/" if path[-1] != "/" else path
    return f"{prefix}katana_vers{version:020d}_rdg.manifest"


def new_part_name(path, dst):
    part_match = part_re.match(dst)
    try:
        version = int(part_match.group("version"))
        node = int(part_match.group("node_id"))
    except AttributeError:
        return None
    prefix = f"{path}/" if path[-1] != "/" else path
    return f"{prefix}part_vers{version:020d}_rdg_node{node:05d}"


def gen_files(dst, cmd_prefix):
    if cmd_prefix == "":
        return os.listdir(dst)
    if cmd_prefix == "aws s3":
        return get_s3_file_list(dst)
    if cmd_prefix == "gsutil":
        return get_gs_file_list(dst)
    # TODO(yasser) support google storage
    return []


def gen_move_commands(dst, dry, cmd_prefix):
    results = []
    if dry is False:
        return "echo '{} ignored: not-dry not implemented'".format(dst)
    for f in gen_files(dst, cmd_prefix):
        old_src = f"{dst}/{f}"
        if dst[-1] == "/":
            old_src = f"{dst}{f}"
        new_dst = new_meta_name(dst, f)
        if new_dst is None:
            new_dst = new_part_name(dst, f)
        if new_dst:
            target_prefix = ""
            if cmd_prefix != "":
                target_prefix = f"{cmd_prefix} "
            results.append(f"{target_prefix}mv {old_src} {new_dst}")
    return results


def process(cmd, dry):
    if cmd[0] == "fs":
        return gen_move_commands(cmd[1], dry, "")
    if cmd[0] == "s3":
        return gen_move_commands(cmd[1], dry, "aws s3")
    if cmd[0] == "gs":
        return gen_move_commands(cmd[1], dry, "gsutil")

    return "echo '{} ignored {} TBD'".format(cmd[1], cmd[0])


inputs = gen_input_list("fs", args.fs) + gen_input_list("s3", args.s3) + gen_input_list("gs", args.gs)
commands = []
for i in inputs:
    res = process(i, ~args.no_dry_run)
    if isinstance(res, list):
        for r in res:
            commands.append(r)
    else:
        commands.append(res)


ideal_cmd_line = " ".join(sys.argv)
print("# the intended usage of this script is to generate RDG migration scripts eg:")
print(f"# {ideal_cmd_line} > migration_script.sh")
print("# inspect the migration script then source it to perform the migration")
for cmd in commands:
    print(cmd)
