# CS5224 Lab 2: Twitter Analytics

Count each user's followers and followees using a Spark data pipeline.

## Source files

- `twitter_counts.py`: distributed counting job for Spark / AWS EMR Serverless.
- `local_counts.py`: independent Python counter for checking results locally.
- `sample_relationships.txt`: small example with duplicates and a self-follow.
- `backend/app.py`: AWS Lambda back end (behind an API Gateway HTTP API).
- `backend/index.html`: web front end, served by the same Lambda at `/`.
- `backend/test_app.py`: local tests for the back end using fake AWS clients.
- `backend/build.sh`: packages `app.py` and `index.html` into `dist/lambda.zip`.

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

## Back end and front end

One Lambda function serves the simple lookup page and coordinates the pipeline.
The public HTTP API only serves the page and user statistics:

| Method and path | Purpose |
| --- | --- |
| `GET /` | Web page |
| `GET /users?ids=A,B[&job=ID]` | Counts from saved Spark results |

Processing is triggered separately using an IAM-authorised Lambda console Test
invocation. It is not exposed as a public HTTP endpoint. Use HTTP API payload
format 2.0 so API Gateway supplies the request context used by the router.
No passcode, dataset-upload page or log-viewing API is needed.

To submit the existing dataset for analysis, use this Test event:

```json
{"action": "start_analysis"}
```

This starts chargeable EMR processing and returns a job ID and a fresh output
prefix. Inspect its status using another Test event:

```json
{"action": "job_status", "job_id": "YOUR_JOB_ID"}
```

Submission refuses to start when it finds an active job. It does not wait for
Spark to finish inside Lambda. Inspect processing logs in AWS. Once the job
succeeds, set `DEFAULT_RESULTS_PREFIX` to the returned output prefix to select
it for normal webpage lookups. Alternatively, request `/users` with `job=ID`.
Lambda caches results per prefix while its execution environment stays warm.

Lambda environment variables:

| Variable | Meaning |
| --- | --- |
| `BUCKET` | Project S3 bucket |
| `APPLICATION_ID` | EMR Serverless application ID |
| `RUNTIME_ROLE_ARN` | IAM role the Spark job runs as |
| `SCRIPT_KEY` | Default `scripts/twitter_counts.py` |
| `DEFAULT_INPUT_KEY` | Default `input/twitter_combined.txt` |
| `DEFAULT_RESULTS_PREFIX` | Results served when no job is given, e.g. `results/run-01/` |

Run the local tests (no AWS account needed):

```sh
cd backend && python3 -m unittest test_app.py
```

## Deployment and IAM

The `infra/` directory contains IAM policy templates:

| File | Attach to / purpose |
| --- | --- |
| `emr-trust-policy.json` | Trust policy for the EMR runtime role |
| `emr-runtime-permissions.json` | Runtime role: read input/script; write results/logs |
| `lambda-trust-policy.json` | Trust policy for the Lambda execution role |
| `lambda-permissions.json` | Lambda role: submit/read EMR jobs, pass the runtime role, read S3 input/results, write its CloudWatch logs |
| `lambda-api-permission.json` | Lambda resource policy: allow this API Gateway API to invoke the function |

Replace `${ACCOUNT_ID}`, `${REGION}`, `${BUCKET_NAME}`, `${APPLICATION_ID}`,
`${EMR_ROLE_NAME}`, `${FUNCTION_NAME}` and `${API_ID}` with your own values before
applying the templates. They contain no credentials and do not create resources.
The API invocation policy is separate from the Lambda execution role.
Your signed-in deployment identity also needs permission to create/configure these
resources; the templates describe the application roles, not an administrator role.

1. Create a private S3 bucket with Block Public Access enabled. Upload the dataset
   to `input/twitter_combined.txt` and the Spark script to `scripts/twitter_counts.py`.
2. Create the EMR runtime role using its trust and permissions templates. Create
   an EMR Serverless Spark application in the same Region, without preinitialised
   workers. This lab used release `emr-spark-8.0`, x86_64.
3. Create the Lambda execution role using its trust and permissions templates.
   Build the package with `sh backend/build.sh`, then upload
   `backend/dist/lambda.zip` to Lambda. Use Python 3.12, x86_64, handler
   `app.lambda_handler`, 512 MB memory and a 30-second timeout.
4. Set Lambda environment variables using `backend/.env.example` as a reference.
   Lambda does not automatically read this file; configure its environment in AWS.
5. Create an API Gateway HTTP API with a Lambda integration, payload format 2.0,
   a `$default` route and an automatically deployed `$default` stage. Allow this
   API to invoke Lambda using the resource-policy template. Keep request context
   intact; no request-parameter mapping is needed. The page and API share an
   origin, so CORS is unnecessary.
6. Invoke the `start_analysis` Test event once, then use `job_status` to check it.
   Once successful, select its output with `DEFAULT_RESULTS_PREFIX`. Open the API
   root URL and check the two required users. Processing incurs cloud usage.

The job uses one driver and two executors, each with 2 vCPU and 4 GB memory,
with dynamic allocation disabled, a 30-minute job timeout and one attempt.
Four partitions are requested for aggregation. Each run writes a new output
prefix. `_SUCCESS` and four `part-*` files indicate completed output.
CloudWatch contains Lambda logs; EMR writes its processing logs under S3 `logs/`.

## Validation status

The local counter has run on the full assignment dataset. Its required-user
counts were independently checked using sets of incoming and outgoing neighbors.
The sample also checks duplicate handling, one-sided users and self-follows.
The Spark job ran on EMR Serverless; its output matched the local counter for
every user. The back end and front end are deployed on Lambda and API Gateway. A direct
Lambda invocation submitted an EMR Serverless job that completed successfully;
its S3 output was retrieved through the public API and returned the expected
counts for both required users. The back end also passes 14 local tests.

## Submission

This repository supplements the assignment PDF. Include the analysis source code
and screenshots of the actual cloud run in the PDF, along with the required
front-end results and short front-end/back-end descriptions.

See the [Apache Spark RDD guide](https://spark.apache.org/docs/3.5.7/rdd-programming-guide.html)
for the distributed operations used here.
