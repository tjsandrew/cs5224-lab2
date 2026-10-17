"""Back end for the Twitter follower-count pipeline (AWS Lambda + API Gateway).

One Lambda function behind an API Gateway HTTP API ($default route) serves
the web page and coordinates the pipeline:

    GET  /                          front-end page (index.html)
    GET  /users?ids=A,B[&job=ID]     follower/followee counts from saved results
IAM-authorised direct Lambda invocations (for example, console Test events):

    {"action": "start_analysis"}     submit the Spark job (incurs AWS usage)
    {"action": "job_status", "job_id": "..."}     inspect a submitted job

These actions have no public HTTP routes. Inspect processing logs in AWS.

Configuration comes from Lambda environment variables (see README).
"""

import json
import os
import re
import uuid
from datetime import datetime, timezone

BUCKET = os.environ.get("BUCKET", "")
APPLICATION_ID = os.environ.get("APPLICATION_ID", "")
RUNTIME_ROLE_ARN = os.environ.get("RUNTIME_ROLE_ARN", "")
SCRIPT_KEY = os.environ.get("SCRIPT_KEY", "scripts/twitter_counts.py")
DEFAULT_INPUT_KEY = os.environ.get("DEFAULT_INPUT_KEY", "input/twitter_combined.txt")
DEFAULT_RESULTS_PREFIX = os.environ.get("DEFAULT_RESULTS_PREFIX", "results/run-01/")

RESULTS_ROOT = "results/"
LOGS_ROOT = "logs/"
MAX_USERS_PER_LOOKUP = 50

# Same worker settings as the successful console run (run-02): one driver and
# two executors, 2 vCPU / 4 GB each, with dynamic allocation switched off.
SPARK_PARAMETERS = (
    "--conf spark.dynamicAllocation.enabled=false "
    "--conf spark.executor.instances=2 "
    "--conf spark.driver.cores=2 --conf spark.driver.memory=4g "
    "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
)
ACTIVE_STATES = ["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING", "QUEUED", "CANCELLING"]

_clients = {}
_results_cache = {}  # results prefix -> {user_id: (followers, followees)}


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


# --------------------------------------------------------------------------
# AWS clients (created lazily so the module can be tested without boto3)
# --------------------------------------------------------------------------

def client(name):
    if name not in _clients:
        import boto3
        _clients[name] = boto3.client(name)
    return _clients[name]


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def respond(status, body, content_type="application/json"):
    if content_type == "application/json":
        body = json.dumps(body, default=str)
    return {
        "statusCode": status,
        "headers": {"Content-Type": content_type, "Cache-Control": "no-store"},
        "body": body,
    }


def s3_uri(key):
    return f"s3://{BUCKET}/{key}"


def key_from_s3_uri(uri):
    """'s3://bucket/a/b' -> 'a/b', only for this bucket."""
    prefix = f"s3://{BUCKET}/"
    if not uri.startswith(prefix):
        raise ApiError(400, "Path is outside this project's bucket.")
    return uri[len(prefix):]


# --------------------------------------------------------------------------
# Results: read Spark's JSON Lines part files from S3
# --------------------------------------------------------------------------

def load_results(prefix):
    """Read every part file under a results prefix into a lookup table.

    Spark writes one part file per reducer; together they hold each user once.
    The table is cached while the Lambda container stays warm.
    """
    if prefix in _results_cache:
        return _results_cache[prefix]

    s3 = client("s3")
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.startswith("part-"):
                keys.append(obj["Key"])
    if not keys:
        raise ApiError(404, f"No results found under {prefix}.")

    counts = {}
    for key in sorted(keys):
        text = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
        for line in text.splitlines():
            if line.strip():
                record = json.loads(line)
                counts[record["user_id"]] = (record["followers"], record["followees"])

    _results_cache[prefix] = counts
    return counts


def parse_user_ids(raw):
    ids = [part.strip() for part in re.split(r"[,\s]+", raw or "") if part.strip()]
    if not ids:
        raise ApiError(400, "Provide at least one user ID, e.g. ?ids=214328887,107830991")
    if len(ids) > MAX_USERS_PER_LOOKUP:
        raise ApiError(400, f"Look up at most {MAX_USERS_PER_LOOKUP} users at a time.")
    for user_id in ids:
        if not user_id.isascii() or not user_id.isdigit():
            raise ApiError(400, f"User IDs must be numeric: {user_id!r}")
    return ids


def get_users(event):
    query = event.get("queryStringParameters") or {}
    ids = parse_user_ids(query.get("ids", ""))

    job_id = query.get("job")
    if job_id:
        job = describe_job(job_id)
        if job["state"] != "SUCCESS":
            raise ApiError(409, f"Job {job_id} has not succeeded (state: {job['state']}).")
        prefix = job["output_prefix"]
        if not prefix or not prefix.startswith(RESULTS_ROOT):
            raise ApiError(400, "Job does not identify a valid results location.")
    else:
        prefix = DEFAULT_RESULTS_PREFIX

    counts = load_results(prefix)
    users = []
    for user_id in ids:
        found = user_id in counts
        followers, followees = counts.get(user_id, (0, 0))
        users.append({
            "user_id": user_id,
            "found": found,
            "followers": followers,
            "followees": followees,
        })
    return respond(200, {"results": s3_uri(prefix), "users": users})


# --------------------------------------------------------------------------
# Jobs: start and monitor the Spark job on EMR Serverless
# --------------------------------------------------------------------------

def output_prefix_from_arguments(arguments):
    try:
        uri = arguments[arguments.index("--output") + 1]
    except (ValueError, IndexError):
        return None
    return key_from_s3_uri(uri).rstrip("/") + "/"


def input_key_from_arguments(arguments):
    try:
        return key_from_s3_uri(arguments[arguments.index("--input") + 1])
    except (ValueError, IndexError, ApiError):
        return None


def describe_job(job_id):
    if not re.fullmatch(r"[A-Za-z0-9]{1,64}", job_id or ""):
        raise ApiError(400, "Invalid job ID.")
    emr = client("emr-serverless")
    try:
        run = emr.get_job_run(applicationId=APPLICATION_ID, jobRunId=job_id)["jobRun"]
    except emr.exceptions.ResourceNotFoundException:
        raise ApiError(404, f"Job {job_id} not found.")

    arguments = run.get("jobDriver", {}).get("sparkSubmit", {}).get("entryPointArguments", [])
    return {
        "job_id": run["jobRunId"],
        "name": run.get("name"),
        "state": run["state"],
        "state_details": run.get("stateDetails", ""),
        "created_at": run.get("createdAt"),
        "updated_at": run.get("updatedAt"),
        "run_time_seconds": run.get("totalExecutionDurationSeconds"),
        "input_key": input_key_from_arguments(arguments),
        "output_prefix": output_prefix_from_arguments(arguments),
    }


def start_job(parameters):
    """Submit processing only from the IAM-authorised direct-invocation path."""
    input_key = parameters.get("input_key") or DEFAULT_INPUT_KEY
    if not isinstance(input_key, str) or not input_key.startswith("input/") or ".." in input_key:
        raise ApiError(400, "input_key must be a file under input/ in the project bucket.")

    s3 = client("s3")
    try:
        s3.head_object(Bucket=BUCKET, Key=input_key)
    except Exception:
        raise ApiError(404, f"Input file not found: {input_key}")

    emr = client("emr-serverless")
    active = emr.list_job_runs(applicationId=APPLICATION_ID, states=ACTIVE_STATES)["jobRuns"]
    if active:
        raise ApiError(409, f"Job {active[0]['id']} is still {active[0]['state']}; wait for it to finish.")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_name = f"api-{stamp}-{uuid.uuid4().hex[:8]}"
    output_key = f"{RESULTS_ROOT}{run_name}"
    response = emr.start_job_run(
        applicationId=APPLICATION_ID,
        executionRoleArn=RUNTIME_ROLE_ARN,
        name=run_name,
        clientToken=str(uuid.uuid4()),
        executionTimeoutMinutes=30,
        retryPolicy={"maxAttempts": 1},
        jobDriver={
            "sparkSubmit": {
                "entryPoint": s3_uri(SCRIPT_KEY),
                "entryPointArguments": [
                    "--input", s3_uri(input_key),
                    "--output", s3_uri(output_key),
                    "--partitions", "4",
                ],
                "sparkSubmitParameters": SPARK_PARAMETERS,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": s3_uri(LOGS_ROOT)}
            }
        },
    )
    return respond(202, {
        "job_id": response["jobRunId"],
        "name": run_name,
        "input_key": input_key,
        "output_prefix": output_key + "/",
    })


# --------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------

def serve_page():
    path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(path, encoding="utf-8") as handle:
        return respond(200, handle.read(), content_type="text/html; charset=utf-8")


def route(method, path, event):
    parts = [p for p in path.split("/") if p]

    if method == "GET" and not parts:
        return serve_page()
    if parts == ["users"] and method == "GET":
        return get_users(event)
    raise ApiError(404, f"No route for {method} {path}")


def lambda_handler(event, context):
    try:
        # API Gateway supplies requestContext outside the HTTP body. Even if
        # a caller puts an action in the body, it only reaches the HTTP router.
        if "requestContext" in event:
            http = event.get("requestContext", {}).get("http", {})
            method = http.get("method", "")
            path = event.get("rawPath") or http.get("path") or "/"
            return route(method, path, event)

        # Direct invocation requires AWS IAM permission to invoke this Lambda.
        # Do not configure an HTTP integration that strips requestContext.
        action = event.get("action")
        if action == "start_analysis":
            return start_job(event)
        if action == "job_status":
            return respond(200, describe_job(event.get("job_id")))
        raise ApiError(400, "Use action start_analysis or job_status for direct invocation.")
    except ApiError as error:
        return respond(error.status, {"error": error.message})
    except Exception as error:  # report unexpected failures without a stack trace
        print(f"Unhandled error: {error!r}")
        return respond(500, {"error": "Internal error; see the Lambda logs."})
