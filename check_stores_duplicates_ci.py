#!/usr/bin/env python3
"""Check stores.json for duplicate Shopify domains without revealing credentials."""
import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict


def fingerprint(value: str) -> str:
    if not value:
        return "(missing)"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?", default="stores.json", help="Path to generated stores.json")
    parser.add_argument(
        "--fail-on-duplicates",
        action="store_true",
        help="Exit with code 1 when duplicate Shopify domains are found (recommended in CI).",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.path):
        print("ERROR: file not found: {}".format(args.path), file=sys.stderr)
        return 2

    try:
        with open(args.path, "r", encoding="utf-8-sig") as handle:
            stores = json.load(handle)
    except json.JSONDecodeError as exc:
        print(
            "ERROR: invalid JSON at line {}, column {}: {}".format(
                exc.lineno, exc.colno, exc.msg
            ),
            file=sys.stderr,
        )
        return 2

    if not isinstance(stores, list):
        print("ERROR: stores.json must contain a JSON array", file=sys.stderr)
        return 2

    by_domain = defaultdict(list)
    for index, store in enumerate(stores, start=1):
        if not isinstance(store, dict):
            print("WARNING: entry #{} is not an object; skipped".format(index))
            continue
        domain = str(store.get("domain") or "").strip().lower()
        if not domain:
            print("WARNING: entry #{} has no domain".format(index))
            continue
        by_domain[domain].append((index, store))

    duplicate_groups = [(domain, rows) for domain, rows in sorted(by_domain.items()) if len(rows) > 1]
    print("Total entries: {}".format(len(stores)))
    print("Unique domains: {}".format(len(by_domain)))
    print("Duplicate domain groups: {}".format(len(duplicate_groups)))

    for domain, rows in duplicate_groups:
        print("\nDUPLICATE DOMAIN: {} ({} entries)".format(domain, len(rows)))
        credential_pairs = set()
        for index, store in rows:
            name = str(store.get("name") or "(no name)")
            client_id = str(store.get("client_id") or "")
            client_secret = str(store.get("client_secret") or "")
            credential_pairs.add((client_id, client_secret))
            print(
                "  entry #{}: name={!r}; client_id_fingerprint={}; client_secret_fingerprint={}".format(
                    index, name, fingerprint(client_id), fingerprint(client_secret)
                )
            )
        print("  distinct credential pairs: {}".format(len(credential_pairs)))
        if len(credential_pairs) > 1:
            print("  ACTION: credentials differ; verify the pair belonging to this exact Shopify app.")
        else:
            print("  ACTION: identical duplicate; keep only one entry.")

    if duplicate_groups and args.fail_on_duplicates:
        print("\nERROR: duplicate Shopify domains found; stopping workflow before main.py.", file=sys.stderr)
        return 1

    if not duplicate_groups:
        print("No duplicate Shopify domains found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
