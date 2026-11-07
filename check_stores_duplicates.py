#!/usr/bin/env python3
"""Safely inspect stores.json for duplicate Shopify domains.

Prints only masked Client ID suffixes and one-way fingerprints of Client Secrets.
Never prints raw Client IDs or Client Secrets.
"""
import hashlib
import json
import os
import sys
from collections import defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else "stores.json"

if not os.path.exists(PATH):
    raise SystemExit(f"ERROR: file not found: {PATH}")

try:
    with open(PATH, "r", encoding="utf-8-sig") as f:
        stores = json.load(f)
except json.JSONDecodeError as exc:
    raise SystemExit(
        f"ERROR: invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
    )

if not isinstance(stores, list):
    raise SystemExit("ERROR: stores.json must be a JSON array")

by_domain = defaultdict(list)
for index, store in enumerate(stores, start=1):
    if not isinstance(store, dict):
        print(f"WARNING: entry #{index} is not an object; skipped")
        continue
    domain = str(store.get("domain") or "").strip().lower()
    if not domain:
        print(f"WARNING: entry #{index} has no domain")
        continue
    by_domain[domain].append((index, store))

print(f"Total entries: {len(stores)}")
print(f"Unique domains: {len(by_domain)}")
print(f"Duplicate domain groups: {sum(1 for v in by_domain.values() if len(v) > 1)}")

has_duplicates = False
for domain, entries in sorted(by_domain.items()):
    if len(entries) < 2:
        continue
    has_duplicates = True
    print(f"\nDUPLICATE DOMAIN: {domain} ({len(entries)} entries)")
    credential_pairs = set()
    for index, store in entries:
        name = str(store.get("name") or "(no name)")
        client_id = str(store.get("client_id") or "")
        client_secret = str(store.get("client_secret") or "")
        client_id_masked = f"…{client_id[-6:]}" if client_id else "(missing)"
        secret_fp = hashlib.sha256(client_secret.encode("utf-8")).hexdigest()[:10] if client_secret else "(missing)"
        credential_pairs.add((client_id, client_secret))
        print(
            f"  entry #{index}: name={name!r}; client_id_suffix={client_id_masked}; "
            f"client_secret_fingerprint={secret_fp}"
        )
    print(f"  distinct credential pairs: {len(credential_pairs)}")
    if len(credential_pairs) > 1:
        print("  ACTION: credentials differ; verify which pair belongs to this exact Shopify app.")
    else:
        print("  ACTION: identical duplicate; keep only one entry.")

if not has_duplicates:
    print("No duplicate Shopify domains found.")
