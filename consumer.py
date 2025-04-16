#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
import time
import csv
import sys
import io
import os
from datetime import datetime
import osmdiff

# epoch in seconds
current_epoch = int(time.time())

nodes_csv = f"nodes_{current_epoch}"
ways_csv = f"ways_{current_epoch}"
relations_csv = f"relations_{current_epoch}"
members_csv = f"members_{current_epoch}"
tags_csv = f"tags_{current_epoch}"

VERBOSE = os.getenv("VERBOSE", "0") == "1"
NODES = os.getenv("NODES", "0") == "1"
WAYS = os.getenv("WAYS", "0") == "1"
RELATIONS = os.getenv("RELATIONS", "0") == "1"
MEMBERS = os.getenv("MEMBERS", "0") == "1"
TAGS = os.getenv("TAGS", "0") == "1"


max_changeset_id = 0

def fetch_xml(url):
    """Fetch XML data from a URL and return its content as bytes."""
    response = requests.get(url)
    if response.status_code == 429:
        print(f"Error: Too many requests to {url} (status code 429)")
        sys.exit(1)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch {url} (status code {response.status_code})")
    return response.content



def write_csv_stdout(rows, fieldnames):
    """Write rows (a list of dictionaries) as CSV to stdout, filtering only allowed fields."""
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    if rows:
        print("DEBUG: first row keys:", list(rows[0].keys()), file=sys.stderr)
    for row in rows:
        # Remove all keys not in fieldnames
        filtered_row = {k: row.get(k, "") for k in fieldnames}
        # Remove extra keys if any exist
        for k in list(row.keys()):
            if k not in fieldnames:
                del row[k]
        writer.writerow(filtered_row)


def main():
    global max_changeset_id
    if len(sys.argv) != 3:
        print("Usage: consumer.py <sequence_number> <etag_output_path>")
        sys.exit(1)

    sequence_number = sys.argv[1]
    etag_output_path = sys.argv[2]

    adiff = osmdiff.AugmentedDiff()
    adiff.sequence_number = sequence_number
    adiff.retrieve()

    try:
        # Transform osmdiff objects to expected CSV dicts
        def to_epoch_millis(ts):
            if ts is None:
                return None
            if isinstance(ts, (int, float)):
                # Assume already ms
                return int(ts)
            # Otherwise, try parsing as seconds
            try:
                return int(float(ts) * 1000)
            except Exception:
                return None

        nodes_rows = []
        ways_rows = []
        relations_rows = []
        members_rows = []
        tags_rows = []
        for o in adiff.create:
            print(o)
            max_changeset_id = max(max_changeset_id, getattr(o, "changeset", 0))
            if isinstance(o, osmdiff.Node):
                row = {
                    "epochMillis": to_epoch_millis(getattr(o, "timestamp", None)),
                    "id": getattr(o, "id", None),
                    "version": getattr(o, "version", None),
                    "changeset": getattr(o, "changeset", None),
                    "username": getattr(o, "user", None),
                    "uid": getattr(o, "uid", None),
                    "lat": getattr(o, "lat", None),
                    "lon": getattr(o, "lon", None),
                }
                nodes_rows.append(row)

        ways_rows = []
        for o in adiff.create:
            max_changeset_id = max(max_changeset_id, getattr(o, "changeset", 0))
            if isinstance(o, osmdiff.Way):
                row = {
                    "epochMillis": to_epoch_millis(getattr(o, "timestamp", None)),
                    "id": getattr(o, "id", None),
                    "version": getattr(o, "version", None),
                    "changeset": getattr(o, "changeset", None),
                    "username": getattr(o, "user", None),
                    "uid": getattr(o, "uid", None),
                    "geometry": getattr(o, "geometry", None),
                }
                ways_rows.append(row)

        relations_rows = []
        for o in adiff.create:
            if isinstance(o, osmdiff.Relation):
                row = {
                    "epochMillis": to_epoch_millis(getattr(o, "timestamp", None)),
                    "id": getattr(o, "id", None),
                    "version": getattr(o, "version", None),
                    "changeset": getattr(o, "changeset", None),
                    "username": getattr(o, "user", None),
                    "uid": getattr(o, "uid", None),
                    "geometry": getattr(o, "geometry", None),
                }
                max_changeset_id = max(max_changeset_id, getattr(o, "changeset", 0) )
                relations_rows.append(row)

        # Members and tags may need similar filtering/flattening if used
        members_rows = [o.members for o in adiff.create if isinstance(o, osmdiff.Relation)]
        tags_rows = [o.tags for o in adiff.create if isinstance(o, osmdiff.Relation)]

        # Write nodes CSV with the specified columns.
        if VERBOSE:
            print("\n--- nodes.csv ---")
        if NODES:
            node_fields = [
                "epochMillis",
                "id",
                "version",
                "changeset",
                "username",
                "uid",
                "lat",
                "lon",
            ]
            write_csv_stdout(nodes_rows, node_fields)

        if WAYS:
            way_fields = [
                "epochMillis",
                "id",
                "version",
                "changeset",
                "username",
                "uid",
                "geometry",
            ]
            write_csv_stdout(ways_rows, way_fields)

        if RELATIONS:
            relation_fields = [
                "epochMillis",
                "id",
                "version",
                "changeset",
                "username",
                "uid",
                "geometry",
            ]
            write_csv_stdout(relations_rows, relation_fields)

        if MEMBERS:
            members_fields = ["relationId", "memberId", "memberRole", "memberType"]
            write_csv_stdout(members_rows, members_fields)

        if TAGS:
            tags_fields = ["epochMillis", "type", "id", "key", "value"]
            write_csv_stdout(tags_rows, tags_fields)

        if VERBOSE:
            print("Processing complete")
            if NODES:
                print(f"Processed {len(nodes_rows)} nodes")
            if WAYS:
                print(f"Processed {len(ways_rows)} ways")
            if RELATIONS:
                print(f"Processed {len(relations_rows)} relations")
            if MEMBERS:
                print(f"Processed {len(members_rows)} members")
            if TAGS:
                print(f"Processed {len(tags_rows)} tags")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"max changeset id: {max_changeset_id}")
    with open(etag_output_path, "w") as fh:
        fh.write(str(max_changeset_id))


if __name__ == "__main__":
    main()
