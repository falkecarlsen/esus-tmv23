import os
from time import sleep
from datetime import datetime, timedelta
import sys
import pandas as pd

# use our bms api and influx adapter
from bms_api_build_aau_dk_v1 import api
from influx_db_adapter import ingest

RUN = "prod"
SAVE_LOC = "prod_data"

TIMESTEP = timedelta(minutes=15)
FETCH_TIME_OVERLAP = timedelta(hours=8)  # worst case seen is 8hrs
MAPPING_PATH = "resources/mapping.csv"


def find_last_fetch_filename():
    # Find the most recent CSV file in the data directory
    last_fetch_time = None
    last_fetch_path = None
    for file in os.listdir(SAVE_LOC):
        if file.endswith(".csv"):
            # assuming ISO8601 format with no timezone
            file_time = datetime.fromisoformat(".".join(file.split(".")[:-1]))
            if last_fetch_time is None or file_time >= last_fetch_time:
                last_fetch_time = file_time
                last_fetch_path = file

    return last_fetch_path


"""
- for each timestep is 15 minutes
    - fetch data from API for the past hour, store in csv
    - load recently fetched csv into wrapper
    - also load next-to-last to deduplicate
    - compute deduplication
    - load deduplicated data into influxdb
"""

if __name__ == "__main__":
    print(f"[{datetime.now()}] Starting ESUS-wrapper")

    # load mapping once per run
    mapping = pd.read_csv(MAPPING_PATH).set_index("externallogid").drop(columns="Unnamed: 0")

    while True:
        print(f"[{datetime.now()}] Starting fetch loop")
        # catch and report ex, but keep running
        try:
            fetch_time = datetime.now()
            # replace whitespace with underscores
            fetch_path = f"{SAVE_LOC}/{fetch_time.__str__().replace(" ", "_")}.csv"

            last_fetch_path = find_last_fetch_filename()

            # fetch from api with rising backoff if logs fail extraction
            failed_logs = ["sentinel"]
            backoff = 1
            no_improvement_count = 0
            while len(failed_logs) > 0:
                print(
                    f"[{fetch_time}] Fetching data from BMS API {backoff=} duration of fetch: {FETCH_TIME_OVERLAP * backoff}"
                )
                failed_logs = api(
                    fetch_time - FETCH_TIME_OVERLAP + timedelta(hours=1) * backoff, fetch_time, fetch_path
                )
                backoff += 1
                print(f"[{datetime.now()}] Failed logs: {len(failed_logs)}, {failed_logs=}", file=sys.stderr)
                if len(failed_logs) == 0:
                    print(f"[{fetch_time}] Fetch successful")
                else:
                    no_improvement_count += 1
                    if no_improvement_count > 0:
                        print(f"[{fetch_time}] WARN: No improvement in fetch, exiting", file=sys.stderr)
                        break

            if last_fetch_path is None:
                print(
                    f"[{fetch_time}] WARN: Last fetch not found, no dedupe available. OK if first run.", file=sys.stderr
                )
                fetch_res = pd.read_csv(fetch_path)
            else:
                print(f"[{fetch_time}] Last fetch found: {last_fetch_path}")
                deduped_csv = f"{SAVE_LOC}/dedupe/deduped_{fetch_time}.csv"
                fetch_old = pd.read_csv(f"{SAVE_LOC}/{last_fetch_path}")
                fetch_new = pd.read_csv(fetch_path)
                # deduplicate by subtracting last fetch from the current fetch
                fetch_res = pd.concat([fetch_new, fetch_old]).drop_duplicates(keep=False)

            # import resulting fetch into influxdb
            if fetch_res is not None and not fetch_res.empty:
                ingest(fetch_res, mapping)
            else:
                print(f"[{datetime.now()}] WARN: No new data to ingest as fetch_res is None/empty", file=sys.stderr)

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            raise

        print(f"[{datetime.now()}] Sleeping until next fetch at {fetch_time + TIMESTEP}")
        # wait until next 15-minute interval
        sleep(TIMESTEP.total_seconds())
