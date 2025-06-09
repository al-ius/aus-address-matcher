#!/usr/bin/env python3
"""File that initialises a DuckDB file from a downloaded GNAF zip file."""

import logging
import os
import sys
import requests
from pathlib import Path
from zipfile import Path as ZipPath, ZipFile

import duckdb
from alive_progress import alive_bar

from common import setup_logging

PROG = Path(__file__).stem
LOG = setup_logging(PROG, fresh=True)
LOG.setLevel(os.getenv("LOG_LEVEL", logging.INFO))

GNAF_INFO_URL = "https://data.gov.au/data/api/3/action/package_show?id=19432f89-dc3a-4ef3-b943-5326ef1dbecc"


# ------------------------------------------------------------------------------
def main() -> int:
    """Initialise a DuckDB file from a downloaded GNAF zip file."""
    if not 1 <= len(sys.argv) <= 2:
        LOG.error("Usage: ./initialise_db.py [DB_FILENAME.db]")
        return 1

    # Check if GNAF zip exists
    gnaf_data = requests.get(GNAF_INFO_URL).json()
    files = gnaf_data.get("result", {}).get("resources", [])

    found_file = None
    for file in files:
        if file.get("description") == "GDA2020":
            found_file = file
            break

    if found_file is None:
        LOG.error("No GDA2020 GNAF file found")
        return 1

    gnaf_file = Path(Path(found_file.get("url")).name)
    if not gnaf_file.is_file():
        LOG.info("Downloading newest GNAF file to: %s", gnaf_file)

        # Download GNAF file is not already downloaded
        with alive_bar(found_file.get("size") // 1024 + 1, unit="KB") as bar:
            res = requests.get(found_file.get("url"), stream=True)
            with open(gnaf_file, "wb") as f:
                for chunk in res.iter_content(1024):
                    f.write(chunk)
                    bar()

    # Check if DuckDB file already exists
    duckdb_file = Path(sys.argv[2] if len(sys.argv) == 3 else "gnaf.db")
    if duckdb_file.is_file():
        LOG.error(
            "File '%s' already exists, use 'rm %s' or use a different filename",
            duckdb_file,
            duckdb_file.name,
        )
        return 1

    # Create DuckDB file
    conn = duckdb.connect(duckdb_file)
    LOG.info("Created DuckDB file '%s'", duckdb_file)

    # Split up relevant files
    zip_file = ZipFile(gnaf_file)
    sql_files = {
        f.split("/")[-1].split(".")[0]: ZipPath(zip_file, f)
        for f in zip_file.namelist()
        if f.endswith(".sql") and "sqlserver" not in f
    }
    std_files = [
        ZipPath(zip_file, f)
        for f in zip_file.namelist()
        if f.endswith(".psv") and "Standard" in f
    ]
    auth_code_files = [
        ZipPath(zip_file, f)
        for f in zip_file.namelist()
        if f.endswith(".psv") and "Authority" in f
    ]

    with conn.cursor() as cursor:
        # Run create tables script
        for query in sql_files["create_tables_ansi"].open().read().lower().split(";"):
            cursor.execute(query)

        # Install DuckDB extension that allows for reading from ZIPs
        cursor.execute("INSTALL zipfs FROM community;")
        cursor.execute("LOAD zipfs;")
        # cursor.execute("INSTALL spatial;")
        # cursor.execute("LOAD spatial;")

        # Load all the Standard files from PSV into corresponding tables
        for file in std_files:
            table_name = "_".join(file.name.split("_")[1:-1]).lower()
            cursor.execute(f"COPY {table_name} FROM 'zip://{file}' (DELIMITER '|');")
            LOG.info("Inserted '%s' in '%s'", file.name, table_name)

        # Load all the Authority Code files from PSV into corresponding tables
        for file in auth_code_files:
            table_name = "_".join(file.name.split("_")[2:-1]).lower()
            cursor.execute(f"COPY {table_name} FROM 'zip://{file}' (DELIMITER '|');")
            LOG.info("Inserted '%s' in '%s'", file.name, table_name)

        # Execute primary key queries to create primary keys in tables
        pk_queries = [
            q.lower()
            for q in sql_files["add_fk_constraints"].open().read().split(";")
            if "PRIMARY KEY" in q
        ]
        for query in pk_queries:
            cursor.execute(query)
        LOG.info("Added primary keys to all tables")

        # Create address summary table, not a view as they are slower
        address_table = (
            sql_files["address_view"]
            .open()
            .read()
            .lower()
            .replace(
                "create or replace view address_view",
                "create table australian_addresses",
            )
        )
        cursor.execute(address_table)
        LOG.info("Created 'australian_addresses' table from data tables")

        cursor.execute(
            """
            create table australian_full_addresses as
            select *,
                regexp_replace(trim(concat_ws(
                    ' ',
                    --building_name,
                    case when flat_number is null then concat(lot_number_prefix, lot_number, lot_number_suffix) end,
                    concat(flat_type, ' ', flat_number_prefix, flat_number, flat_number_suffix),
                    concat(level_type, ' ', level_number_prefix, level_number, level_number_suffix),
                    case
                        when number_last is null
                            then concat(number_first_prefix, number_first, number_first_suffix)
                        else concat(
                                number_first_prefix, number_first, number_first_suffix, '-',
                                number_last_prefix, number_last, number_last_suffix
                             )
                    end,
                    concat_ws(' ', street_name, street_type_code, street_suffix_type),
                    locality_name,
                    state_abbreviation,
                    postcode
                )), '\s{2,}', ' ', 'g') as address
            from australian_addresses;            
            """
        )
        LOG.info("Created 'australian_full_addresses' from 'australian_addresses'")

        cursor.execute(
            "create index index_aus_full_addr_street_locality_pid on australian_full_addresses (street_locality_pid);"
        )
        LOG.info(
            "Created 'index_aus_full_addr_street_locality_pid' on 'australian_full_addresses'"
        )

        cursor.execute(
            """
            create table street_lookup as
            select sc.street_locality_pid,
                   sc.street_name,
                   sc.street_type_code,
                   sc.street_type,
                   sc.street_suffix_code,
                   sc.street_suffix,
                   sc.locality_pid,
                   ad.locality_name,
                   s.state_abbreviation,
                   ad.postcode,
                   concat_ws(' ', sc.street_full_name, ad.locality_name, s.state_abbreviation, ad.postcode) as address_suffix
            from (
                select sl.*,
                       sl.street_type_code as street_type,
                       sl.street_suffix_code AS street_suffix,
                       concat_ws(' ', sl.street_name, sl.street_type_code, sl.street_suffix_code) as street_full_name
                from street_locality sl
                union
                select sl.*,
                       sl.street_type_code as street_type,
                       ss.name AS street_suffix,
                       concat_ws(' ', sl.street_name, sl.street_type_code, ss.name) as street_full_name
                from street_locality sl
                left join street_suffix_aut ss ON sl.street_suffix_code = ss.code
                union
                select sl.*,
                       st.name as street_type,
                       sl.street_suffix_code AS street_suffix,
                       concat_ws(' ', sl.street_name, st.name, sl.street_suffix_code) as street_full_name
                from street_locality sl
                join street_type_aut st ON sl.street_type_code = st.code
                union
                select sl.*,
                       st.name as street_type,
                       ss.name AS street_suffix,
                       concat_ws(' ', sl.street_name, st.name, ss.name) as street_full_name
                from street_locality sl
                join street_type_aut st ON sl.street_type_code = st.code
                left join street_suffix_aut ss ON sl.street_suffix_code = ss.code
            ) sc
            join state s on prefix(sc.street_locality_pid, s.state_abbreviation)
            join (
                select distinct street_locality_pid, locality_name, postcode
                from address_detail
                join locality using (locality_pid)
            ) ad on ad.street_locality_pid = sc.street_locality_pid;
            """
        )
        LOG.info("Created 'street_lookup' from GNAF tables")

    conn.close()
    LOG.info("Closed connection to DuckDB file")
    return 0


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # exit(main())  # Uncomment for debugging
    try:
        exit(main())
    except InterruptedError:
        LOG.info("Keyboard interrupt. exiting...")
    except Exception as e:
        LOG.info("Error occurred during runtime: %s", e)
    finally:
        LOG.info("Done")
