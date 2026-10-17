# CS5224 Lab 2: Twitter Analytics

Count each user's followers and followees using a Spark data pipeline.

## Source files

- `twitter_counts.py`: distributed counting job for Spark / AWS EMR Serverless.
- `local_counts.py`: independent Python counter for checking results locally.
- `sample_relationships.txt`: small example with duplicates and a self-follow.

The API back end and web front end will be added as they are implemented.

## Input and counting rules

Each input line contains two numeric IDs: `follower_id followee_id`.
For example, `101 202` means user 101 follows user 202.

Duplicate pairs count once. Self-follow pairs are retained. Users appearing on
only one side are included with zero for the other count. Blank or malformed
records cause an error. IDs are stored as strings.

The assignment dataset is not distributed in this repository. Supply it separately.

## Run the local counter

Requires Python 3; no third-party packages are needed. Run from this directory:

```sh
python3 local_counts.py \
  --input sample_relationships.txt \
  --output output/local-sample.jsonl
```

For the sample, the expected counts are:

| User | Followers | Followees |
| --- | ---: | ---: |
| 101 | 0 | 2 |
| 202 | 2 | 0 |
| 303 | 0 | 1 |
| 404 | 1 | 0 |
| 505 | 1 | 1 |

## Run the Spark job

Requires a Spark environment with PySpark and its supported Python/Java runtime.
EMR Serverless supplies Spark and PySpark; no extra Python packages are used.
For an existing local Spark installation:

```sh
spark-submit --master 'local[2]' twitter_counts.py \
  --input sample_relationships.txt \
  --output output/spark-sample \
  --partitions 4
```

For EMR Serverless, upload `twitter_counts.py` as the job's entry point and pass
`--input s3://YOUR_BUCKET/input/twitter_combined.txt`,
`--output s3://YOUR_BUCKET/results/run-01` and `--partitions 4` as script arguments.
The runtime role needs permission to read the input and write the results.

Spark parses and deduplicates pairs, caches them for reuse, counts each direction
with `map` and `reduceByKey`, and combines the counts with `fullOuterJoin`.
It writes JSON Lines to multiple `part-*` files without collecting all results
onto the driver. Output order is unspecified. Both scripts require a new output
path rather than overwriting previous results.

## Validation status

The local counter has run on the full assignment dataset. Its required-user
counts were independently checked using sets of incoming and outgoing neighbors.
The sample also checks duplicate handling, one-sided users and self-follows.
The Spark pipeline and AWS deployment have not yet been run.

## Submission

This repository supplements the assignment PDF. Include the analysis source code
and screenshots of the actual cloud run in the PDF, along with the required
front-end results and short front-end/back-end descriptions.

See the [Apache Spark RDD guide](https://spark.apache.org/docs/3.5.7/rdd-programming-guide.html)
for the distributed operations used here.
