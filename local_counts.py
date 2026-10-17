"""Calculate local reference counts before running the Spark job on AWS.

Uses only Python's standard library. This is an independent counting version,
not a Spark run or evidence of cloud execution. Duplicate pairs are removed;
self-follows are retained, matching the current Spark script's cleaning rule.
"""

import argparse
import json
from collections import Counter
from pathlib import Path
from time import perf_counter


def calculate_counts(input_path):
    """Read once, skip previously seen pairs, and count both directions."""
    seen_pairs = set()
    followers = Counter()
    followees = Counter()
    rows = 0
    self_follows = 0

    with input_path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            fields = line.split()
            if len(fields) != 2 or not all(
                field.isascii() and field.isdigit() for field in fields
            ):
                raise ValueError(f"Malformed record on line {line_number}: {line!r}")

            rows += 1
            follower_id, followee_id = fields
            pair = (follower_id, followee_id)
            if pair in seen_pairs:
                continue

            seen_pairs.add(pair)
            followees[follower_id] += 1
            followers[followee_id] += 1
            if follower_id == followee_id:
                self_follows += 1

    # Keep users who appear on only one side; Counter supplies missing zeros.
    users = followers.keys() | followees.keys()
    results = {
        user_id: {
            "user_id": user_id,
            "followers": followers[user_id],
            "followees": followees[user_id],
        }
        for user_id in users
    }
    unique_pairs = len(seen_pairs)
    assert sum(followers.values()) == unique_pairs
    assert sum(followees.values()) == unique_pairs

    summary = {
        "input_rows": rows,
        "unique_relationships": unique_pairs,
        "duplicate_rows_removed": rows - unique_pairs,
        "unique_self_follows_retained": self_follows,
        "users": len(users),
        "total_followers": sum(followers.values()),
        "total_followees": sum(followees.values()),
    }
    return results, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new output path to preserve previous reference results.")

    started = perf_counter()
    results, summary = calculate_counts(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as destination:
        for user_id in sorted(results, key=int):
            destination.write(json.dumps(results[user_id]) + "\n")

    summary["elapsed_seconds"] = round(perf_counter() - started, 3)
    summary["output"] = str(args.output)
    summary["required_users"] = [
        results.get(user_id) for user_id in ("214328887", "107830991")
    ]
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
