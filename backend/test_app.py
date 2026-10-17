"""Local tests for app.py using fake AWS clients (no AWS account or boto3 needed).

Run from this folder:  python3 -m unittest test_app.py
"""

import io
import json
import os
import unittest

os.environ.update({
    "BUCKET": "test-bucket",
    "APPLICATION_ID": "app123",
    "RUNTIME_ROLE_ARN": "arn:aws:iam::000000000000:role/runtime",
})

import app  # noqa: E402  (environment must be set first)


class NotFound(Exception):
    pass


class FakeS3:
    def __init__(self, objects):
        self.objects = objects  # key -> bytes

    def get_paginator(self, name):
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                keys = [k for k in fake.objects if k.startswith(Prefix)]
                yield {"Contents": [{"Key": k} for k in keys]}
        return Paginator()

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise NotFound(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise NotFound(Key)
        return {}



class FakeEmr:
    class exceptions:
        ResourceNotFoundException = NotFound

    def __init__(self, active=None):
        self.active = active or []
        self.started = None
        self.runs = {
            "job1": {
                "jobRunId": "job1", "name": "web-1", "state": "SUCCESS",
                "jobDriver": {"sparkSubmit": {"entryPointArguments": [
                    "--input", "s3://test-bucket/input/data.txt",
                    "--output", "s3://test-bucket/results/web-1",
                    "--partitions", "4"]}},
            },
            "job2": {"jobRunId": "job2", "name": "web-2", "state": "RUNNING",
                     "jobDriver": {"sparkSubmit": {"entryPointArguments": []}}},
        }

    def get_job_run(self, applicationId, jobRunId):
        if jobRunId not in self.runs:
            raise NotFound(jobRunId)
        return {"jobRun": self.runs[jobRunId]}

    def list_job_runs(self, applicationId, states=None, maxResults=None):
        if states:
            return {"jobRuns": self.active}
        return {"jobRuns": [{"id": r["jobRunId"], "name": r["name"], "state": r["state"]}
                            for r in self.runs.values()]}

    def start_job_run(self, **kwargs):
        self.started = kwargs
        return {"jobRunId": "newjob"}


def lines(*records):
    return "\n".join(json.dumps(r) for r in records).encode()


def event(method, path, query=None, body=None):
    headers = {}
    return {
        "rawPath": path,
        "requestContext": {"http": {"method": method}},
        "queryStringParameters": query,
        "headers": headers,
        "body": json.dumps(body) if body is not None else None,
    }


def call(evt):
    result = app.lambda_handler(evt, None)
    is_json = result["headers"]["Content-Type"] == "application/json"
    return result["statusCode"], json.loads(result["body"]) if is_json else result["body"]


class AppTests(unittest.TestCase):
    def setUp(self):
        app._results_cache.clear()
        self.s3 = FakeS3({
            # Default results split across two part files, as Spark writes them.
            "results/run-01/part-00000": lines(
                {"user_id": "214328887", "followers": 180, "followees": 214}),
            "results/run-01/part-00001": lines(
                {"user_id": "107830991", "followers": 180, "followees": 157}),
            "results/run-01/_SUCCESS": b"",
            "results/web-1/part-00000": lines({"user_id": "5", "followers": 1, "followees": 2}),
            "input/twitter_combined.txt": b"1 2\n",
        })
        self.emr = FakeEmr()
        app._clients.clear()
        app._clients.update({"s3": self.s3, "emr-serverless": self.emr})

    def test_page_is_served(self):
        status, body = call(event("GET", "/"))
        self.assertEqual(status, 200)
        self.assertIn("Twitter Follower Counts", body)

    def test_required_users_from_default_results(self):
        status, body = call(event("GET", "/users", {"ids": "214328887, 107830991"}))
        self.assertEqual(status, 200)
        found = {u["user_id"]: (u["followers"], u["followees"]) for u in body["users"]}
        self.assertEqual(found, {"214328887": (180, 214), "107830991": (180, 157)})

    def test_unknown_user_is_reported(self):
        status, body = call(event("GET", "/users", {"ids": "999"}))
        self.assertEqual(status, 200)
        self.assertFalse(body["users"][0]["found"])

    def test_non_numeric_ids_rejected(self):
        status, _ = call(event("GET", "/users", {"ids": "abc"}))
        self.assertEqual(status, 400)

    def test_users_from_a_specific_successful_job(self):
        status, body = call(event("GET", "/users", {"ids": "5", "job": "job1"}))
        self.assertEqual(status, 200)
        self.assertEqual(body["users"][0]["followees"], 2)

    def test_users_from_unfinished_job_rejected(self):
        status, _ = call(event("GET", "/users", {"ids": "5", "job": "job2"}))
        self.assertEqual(status, 409)

    def test_start_job_uses_existing_setup(self):
        status, body = call({"action": "start_analysis"})
        self.assertEqual(status, 202)
        started = self.emr.started
        self.assertEqual(started["applicationId"], "app123")
        self.assertEqual(started["executionRoleArn"], os.environ["RUNTIME_ROLE_ARN"])
        driver = started["jobDriver"]["sparkSubmit"]
        self.assertEqual(driver["entryPoint"], "s3://test-bucket/scripts/twitter_counts.py")
        self.assertIn("s3://test-bucket/input/twitter_combined.txt", driver["entryPointArguments"])
        self.assertIn("spark.dynamicAllocation.enabled=false", driver["sparkSubmitParameters"])
        self.assertEqual(body["output_prefix"], body["name"].join(["results/", "/"]))

    def test_start_job_refused_while_another_runs(self):
        self.emr.active = [{"id": "job2", "state": "RUNNING"}]
        status, _ = call({"action": "start_analysis"})
        self.assertEqual(status, 409)
        self.assertIsNone(self.emr.started)

    def test_start_job_rejects_paths_outside_input(self):
        status, _ = call({"action": "start_analysis", "input_key": "results/x"})
        self.assertEqual(status, 400)

    def test_job_status_includes_output(self):
        status, body = call({"action": "job_status", "job_id": "job1"})
        self.assertEqual(status, 200)
        self.assertEqual(body["state"], "SUCCESS")
        self.assertEqual(body["output_prefix"], "results/web-1/")

    def test_http_cannot_start_processing(self):
        request = event("POST", "/jobs", body={"action": "start_analysis"})
        request["action"] = "start_analysis"
        self.assertEqual(call(request)[0], 404)
        self.assertIsNone(self.emr.started)

    def test_http_body_action_does_not_override_lookup(self):
        request = event("GET", "/users", {"ids": "214328887"},
                        body={"action": "start_analysis"})
        self.assertEqual(call(request)[0], 200)
        self.assertIsNone(self.emr.started)

    def test_removed_routes_are_unavailable(self):
        for method, path in [("POST", "/uploads"), ("GET", "/jobs"),
                             ("GET", "/jobs/job1"), ("GET", "/jobs/job1/logs")]:
            self.assertEqual(call(event(method, path))[0], 404)

    def test_unknown_job_and_action(self):
        self.assertEqual(call({"action": "job_status", "job_id": "nope"})[0], 404)
        self.assertEqual(call({"action": "job_status", "job_id": "bad$id"})[0], 400)
        self.assertEqual(call({"action": "job_status"})[0], 400)
        self.assertEqual(call({"action": "unknown"})[0], 400)
        self.assertEqual(call({})[0], 400)


if __name__ == "__main__":
    unittest.main()
