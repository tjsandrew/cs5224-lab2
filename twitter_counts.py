"""Count Twitter followers and followees with Spark.

Input:  one 'follower_id followee_id' relationship per line.
Output: JSON records with user_id, followers and followees, in part files.

Cleaning rule: count each distinct relationship once. Self-follow pairs are
retained because the assignment does not explicitly require their removal.

Run through spark-submit (or EMR Serverless), passing --input and --output.
Use a new output path for each run; Spark will not overwrite existing results.
"""

import argparse
import json
from operator import add


def parse_relationship(line):
    """Turn '123 456' into ('123', '456'); reject malformed records."""
    fields = line.split()
    if len(fields) != 2 or not all(field.isascii() and field.isdigit() for field in fields):
        raise ValueError(f"Expected two numeric user IDs, received: {line!r}")

    follower_id, followee_id = fields
    return follower_id, followee_id


def map_follower(relationship):
    """If A follows B, B gains one follower: emit (B, 1)."""
    follower_id, followee_id = relationship
    return followee_id, 1


def map_followee(relationship):
    """If A follows B, A gains one followee: emit (A, 1)."""
    follower_id, followee_id = relationship
    return follower_id, 1


def count_users(unique_relationships, partitions):
    """Ask Spark to count both directions, then combine them by user ID."""
    # reduceByKey(add) sums each user's ones. Spark combines partial counts
    # on workers before transferring them, reducing network traffic.
    followers = unique_relationships.map(map_follower).reduceByKey(add, partitions)
    followees = unique_relationships.map(map_followee).reduceByKey(add, partitions)

    # A full outer join keeps users who appear on only one side of the file.
    # Example: ('123', (None, 2)) means no followers and two followees.
    return followers.fullOuterJoin(followees, partitions)


def format_result(user_counts):
    """Convert one joined result to JSON, replacing missing counts with zero."""
    user_id, (followers, followees) = user_counts
    return json.dumps({
        "user_id": user_id,
        "followers": followers if followers is not None else 0,
        "followees": followees if followees is not None else 0,
    })


def read_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input file path, usually s3://...")
    parser.add_argument("--output", required=True, help="New output directory, usually s3://...")
    parser.add_argument(
        "--partitions", type=int, default=4,
        help="Minimum input partitions and partitions for aggregation (default: 4)",
    )
    args = parser.parse_args()
    if args.partitions < 1:
        parser.error("--partitions must be at least 1")
    return args


def main():
    # Import here so the small Python functions can be reviewed/tested without
    # installing Spark. The actual job must run in a Spark environment.
    from pyspark.sql import SparkSession

    args = read_arguments()
    spark = SparkSession.builder.appName("TwitterFollowerCounts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read the original file in partitions; IDs remain strings.
        lines = spark.sparkContext.textFile(args.input, minPartitions=args.partitions)
        relationships = lines.map(parse_relationship)

        # 2. Remove duplicate PAIRS, not duplicate users. Cache the cleaned
        #    pairs because both counting operations use them.
        unique_relationships = relationships.distinct(args.partitions).cache()

        try:
            # 3. Count followers and followees for every user.
            counts = count_users(unique_relationships, args.partitions)

            # 4. Write JSON part files directly from workers to storage.
            #    No collect(): the full result never needs to fit on the driver.
            counts.map(format_result).saveAsTextFile(args.output)
            print(f"Counts saved to {args.output}")
        finally:
            unique_relationships.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
