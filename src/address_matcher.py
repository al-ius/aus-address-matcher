import logging
import os
import re
import sys
import time
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any, Dict, Iterable, List, Optional

import duckdb
from Levenshtein import ratio

from common import setup_logging

PROG = Path(__file__).stem
LOG = setup_logging(PROG, fresh=True)
LOG.setLevel(os.getenv("LOG_LEVEL", logging.INFO))

DUCKDB_FILENAME = "gnaf.db"

STREET_FIND_QUERY = """
    WITH matched_street AS MATERIALIZED (
        SELECT DISTINCT street_locality_pid, locality_pid, street_name, street_type_code,
            street_suffix_code, state_abbreviation, locality_name, postcode,
            contains(' ' || $1 || ' ', ' ' || street_name || ' ')::int * -3.1 as street_score
        FROM street_lookup
        ORDER BY levenshtein($1, address_suffix) + street_score - jaro_winkler_similarity($1, address_suffix)
        LIMIT 20
    )
    SELECT DISTINCT
        sl.street_locality_pid, sl.street_name, sl.street_type_code,
        sl.street_suffix_code, ts.state_abbreviation, ts.locality_name, ts.postcode
    FROM matched_street ts
    LEFT JOIN locality_neighbour ln ON ts.locality_pid = ln.locality_pid
    LEFT JOIN street_locality sl ON sl.street_name = ts.street_name
        AND (ts.street_type_code IS NULL OR sl.street_type_code = ts.street_type_code)
        AND (sl.locality_pid = ln.locality_pid OR sl.locality_pid = ln.neighbour_locality_pid)
        AND (ts.street_suffix_code IS NULL OR sl.street_suffix_code = ts.street_suffix_code);
    """

STREET_SEARCH_QUERY = """
    SELECT address_detail_pid, street_name, address, postcode
    FROM australian_full_addresses
    WHERE street_locality_pid IN ({placeholders});
    """


def db_query_get_dict(
    cur: duckdb.DuckDBPyConnection, query: str, data: Iterable[Any], debug: bool = False
) -> List[Dict[str, Any]]:
    """Query database and get rows back as a dict."""

    start_time = time.perf_counter()
    cur.execute(query, data)
    cols = [c[0] for c in cur.description]
    output_rows = []
    for row in cur.fetchall():
        output_rows.append(dict(zip(cols, row)))
    if debug:
        LOG.debug(f"Query took %.2 seconds", time.perf_counter() - start_time)

    return output_rows


def sub_from_back(pattern: str, replacement: str, text: str) -> str:
    """String replaces first occurrence from the back"""
    pattern_reversed = pattern[::-1]
    reversed_text = text[::-1]
    replaced_reversed = re.sub(pattern_reversed, replacement, reversed_text)
    return replaced_reversed[::-1]


def match_address(
    cursor: duckdb.DuckDBPyConnection, address: str, debug: bool = False
) -> Optional[Dict[str, Any]]:
    """Address matching logic."""

    matched_streets = db_query_get_dict(cursor, STREET_FIND_QUERY, (address,), debug)
    street_codes = [s.get("street_locality_pid") for s in matched_streets]
    postcodes = set(s.get("postcode") for s in matched_streets)

    if len(street_codes) == 0:
        LOG.info("No streets found, skipping")
        return None

    # Find all addresses on the street
    id_placeholders = ", ".join("?" * len(street_codes))
    placeholder_query = STREET_SEARCH_QUERY.format(placeholders=id_placeholders)
    addresses_to_search = db_query_get_dict(
        cursor, placeholder_query, street_codes, debug
    )

    # Remove postcode and get numbers in string
    start_time = time.perf_counter()
    search_str = address
    for p in postcodes:
        search_str = sub_from_back(p, "", search_str)

    split_chars = "\s!\"#$%&'\(\)*+,./:;<=>?@[\]^_`\{|\}~-"
    split_pattern = f"[A-Z{split_chars}]+"
    number_tokens = [
        t for t in re.split(split_pattern, search_str) if t and not t.isalpha()
    ]

    candidates = []
    output_rankings = []
    for s in addresses_to_search:
        s_search_str = " " + re.sub(s.get("postcode", ""), "", s.get("address", ""))
        search_number_tokens = [
            t for t in re.split(split_pattern, s_search_str) if t and not t.isalpha()
        ]
        matches = list(filter(lambda x: x in search_number_tokens, number_tokens))
        non_match_count = len(
            list(filter(lambda x: x not in number_tokens, search_number_tokens))
        )
        position_score = sum(
            list((search_number_tokens.index(m) + len(m)) * 0.1 for m in matches)
        )
        similarity_score = ratio(address, s.get("address", "")) * 2
        street_name_score = int(f" {s.get('street_name', '')} " in address)
        number_match_score = (
            street_name_score
            + similarity_score
            + len(matches)
            + position_score
            - 0.1 * non_match_count
        )

        if debug:
            output_rankings.append(f"[{number_match_score:4.2f}] {s.get('address')}")

        if number_match_score > 0:
            s["similarity"] = similarity_score
            s["match_score"] = number_match_score
            candidates.append((s, number_match_score))

    if debug:
        output_rankings.sort(reverse=True)
        for row in output_rankings[:10]:
            print(row)

    if not candidates:
        LOG.info("No addresses found")
        return None

    highest_score = max(c[1] for c in candidates)
    possible_candidates = [c[0] for c in candidates if c[1] == highest_score]
    highest_matching = max(possible_candidates, key=lambda c: c.get("match_score"))
    LOG.info(
        "[%4.2f] %s,%s",
        highest_matching.get("match_score"),
        highest_matching.get("address"),
        highest_matching.get("address_detail_pid"),
    )

    if debug:
        LOG.debug("Scoring time:", time.perf_counter() - start_time)
    return highest_matching


def validator_thread_method(
    address_queue: Queue, output_queue: Queue, debug: bool = False
) -> None:
    """Thread method to validate addresses."""

    LOG.info("Thread started")
    conn = duckdb.connect(DUCKDB_FILENAME, read_only=True)
    LOG.info("Connected to database: %s", DUCKDB_FILENAME)

    with conn.cursor() as cur:
        while True:
            # Get address from queue
            address = address_queue.get()

            # Exit case
            if address is None:
                break

            matched_address = match_address(cur, address, debug)
            output_queue.put(matched_address)

    LOG.info("Thread shutting down")


def main(address: str = None) -> int:
    """Do the business."""

    if address is not None:
        addresses = [address.upper()]
    else:
        addresses: List[str] = open("src/test/data/sample_addresses.txt").readlines()
        addresses = [
            a[: a.find("--")].strip().upper() for a in addresses if not a.startswith("--")
        ]

    address_queue = Queue()
    output_queue = Queue()

    debug = len(addresses) == 1

    thread_count = min(len(addresses) // 20 + 1, 8)

    for address in addresses:
        address_queue.put(address)

    if thread_count > 1:
        threads = []
        for _ in range(thread_count):
            thread = Thread(
                target=validator_thread_method, args=(address_queue, output_queue, debug)
            )
            threads.append(thread)
            thread.start()
            address_queue.put(None)

        for thread in threads:
            thread.join()

    else:
        address_queue.put(None)
        validator_thread_method(address_queue, output_queue, debug)

    return 0


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) >= 1:
        exit(main(" ".join(args)))

    exit(main())
