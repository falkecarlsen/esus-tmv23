"""
How to check that connection credentials are suitable for queries and writes from/into specified bucket.
"""
from pprint import pprint

import influxdb_client
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
from influxdb_client.rest import ApiException
from influxdb_client import Point
import pandas as pd


"""
Define credentials
"""
url = "http://localhost:8086"
token = "46x4U9VjX6FMluFpRh8Ukw5EeyRVgXevKyecLuMIRKyQiGPj5RLkkj5Ko75Nj70ptvcYTjJW1dcPdrLEt8ruZA=="
org = "my-org"
bucket = "tmv23-3"


def check_connection():
    """Check that the InfluxDB is running."""
    print("> Checking connection ...", end=" ")
    client.api_client.call_api("/ping", "GET")
    print("ok")


def check_query():
    """Check that the credentials has permission to query from the Bucket"""
    print("> Checking credentials for query ...", end=" ")
    try:
        client.query_api().query(f'from(bucket:"{bucket}") |> range(start: -1m) |> limit(n:1)', org)
    except ApiException as e:
        # missing credentials
        if e.status == 404:
            raise Exception(
                f"The specified token doesn't have sufficient credentials to read from '{bucket}' "
                f"or specified bucket doesn't exists."
            ) from e
        raise
    print("ok")


def check_write():
    """Check that the credentials has permission to write into the Bucket"""
    print("> Checking credentials for write ...", end=" ")
    try:
        client.write_api(write_options=SYNCHRONOUS).write(bucket, org, b"")
    except ApiException as e:
        # bucket does not exist
        if e.status == 404:
            raise Exception(f"The specified bucket does not exist.") from e
        # insufficient permissions
        if e.status == 403:
            raise Exception(f"The specified token does not have sufficient credentials to write to '{bucket}'.") from e
        # 400 (BadRequest) caused by empty LineProtocol
        if e.status != 400:
            raise
    print("ok")


with InfluxDBClient(url=url, token=token, org=org) as client:
    check_connection()
    check_query()
    check_write()

# done testing connection

from datetime import datetime

time_start = datetime.now()

# read df
df = pd.read_csv("output/TMV23_20240115_080000_20240402_115149.csv")
# do datetime conversion for timestamp to align with Point time-format
df["timestamp"] = pd.to_datetime(df["timestamp"])

# drop Unnamed: 0 due to CSV serialisation
df.drop(columns="Unnamed: 0")

# replace spaces in source with underscores
df["source"] = df["source"].str.replace(" ", "_")

# get mapping fixme: unused currently
mapping = (
    pd.read_csv("output/mapping.csv").set_index("externallogid").drop(columns="Unnamed: 0").to_dict("dict")["source"]
)

time_computation = datetime.now()

print(f"Time to read and pre-compute on dataframe: {time_computation - time_start}")


def dataframe_to_influxdb_points(dataframe: pd.DataFrame):
    for _, row in dataframe.iterrows():
        yield Point(row["source"]).field("value", row["value"]).time((row["timestamp"]))


with InfluxDBClient(url=url, token=token, org=org) as client:
    # write points
    step = 100_000
    i = 0
    with client.write_api(write_options=WriteOptions(batch_size=10_000, flush_interval=1_000)) as con:
        for point in dataframe_to_influxdb_points(df):
            i += 1
            con.write(bucket, org, point)
            if i % step == 0:
                print(".", end="")
            if i % (step * 10) == 0:
                print(f"\n{i / len(df):.3%} or {i}/{len(df)} points written")

        time_write = datetime.now()
        print(f"\nTime to write: {time_write - time_computation}")

    exit()
    query = f'from(bucket: "{bucket}") |> range(start: -1h)'

    # Query data from InfluxDB
    tables = client.query_api().query(query, org=org)

    # Process the result
    for table in tables:
        for record in table.records:
            # Assuming record has fields and time
            print(f"Time: {record.get_field()['_time']}, Fields: {record.get_field()}")
