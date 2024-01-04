import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import boto3


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        "-b",
        "--bucket-name",
        metavar="b",
        action="store",
        type=str,
        help="s3 bucket name",
        required=True,
    )
    parser.add_argument(
        "-p",
        "--prefix",
        metavar="p",
        action="store",
        type=str,
        help="s3 bucket prefix",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output-path",
        metavar="e",
        action="store",
        type=str,
        help="location where file name files will be written",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--timestring",
        metavar="t",
        action="store",
        type=str,
        help="ISO format time string in format +%F %T",
        required=False,
    )

    return parser.parse_args()


def get_keys_list(client: boto3.client, bucket_name: str, prefix: str) -> List:
    paginator = client.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    keys = []
    for page in page_iterator:
        if "Contents" in page:
            for key in page["Contents"]:
                keys.append((key["Key"], key["LastModified"], key["Size"]))
    return keys if keys else []


def delete_prefix(bucket_name: str, prefix: str):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()


def check_keys(key_list: List, process_start_date, logger):
    num_old_keys = 0
    num_new_keys = 0

    for key_name, key_date, _ in key_list:

        if process_start_date and key_date < process_start_date:
            num_old_keys += 1
        else:
            num_new_keys += 1

    logger.info(f"Timestring: {process_start_date}")
    logger.info(f"  Number of old files in directory:{num_old_keys}")
    logger.info(f"  Number of new files generated:{num_new_keys}")

    return num_old_keys, num_new_keys


def generate_size_stats(key_list: List, logger):

    sizes = sorted([k[2] for k in key_list])
    num_sizes = len(sizes)

    logger.info("size stats\tsize(in bytes)")
    logger.info("-" * 20)

    logger.info(f"\tMIN:\t{sizes[0]}")
    for p in [10, 25, 50, 75, 90]:
        index = int(p / 100.0 * num_sizes)
        logger.info(f"\tp{p}\t{sizes[index]}")
    logger.info(f"\tMAX:\t{sizes[-1]}")


def get_id_group(key_str: str) -> Optional[str]:
    if "_SUCCES" not in key_str:
        try:
            return key_str.split("/")[-2].split("=")[1]
        except IndexError:
            raise ValueError(
                "Bucket contains key that is not like 'id_group=<int>'"
            )
    else:
        return None


def log_keys(key_list: List, s3_path: str, logger) -> None:
    for key in key_list:
        logger.info(f"keys in {s3_path}")
        logger.info(key)


def write_keys(out_path: Path, keys_list: List):
    out_path.mkdir(exist_ok=True)
    for key, _, _ in keys_list:
        id_group = get_id_group(key)
        if id_group is not None:
            key_path = out_path / f"id_group={id_group}"
            with key_path.open("w") as f:
                f.write(key)


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    args = parse_args()

    s3_client = boto3.client("s3")

    keys = get_keys_list(s3_client, args.bucket_name, args.prefix)

    if len(keys) == 0:
        logger.error(f"No keys found in {args.bucket_name}/{args.prefix}")
        raise ValueError("Empty bucket or bucket does not exist")

    if args.timestring:
        timestamp = datetime.fromisoformat(args.timestring)
    else:
        timestamp = None

    num_old_keys, num_new_keys = check_keys(keys, timestamp, logger)
    if num_old_keys >= 1:
        logger.warning("old keys on system")
    if num_new_keys == 0:
        logger.error("no new files generated")
        raise ValueError("No New Files generated")

    generate_size_stats(keys, logger)
    try:
        write_keys(Path(args.output_path), keys)
        logger.info(f"successfully wrote files to {args.output_path}")
    except ValueError:
        log_keys(keys, f"{args.bucket_name}/{args.prefix}", logger)
        delete_prefix(args.bucket_name, args.prefix)
        raise ValueError("Specified s3 path has unexpected keys")


if __name__ == "__main__":
    main()
