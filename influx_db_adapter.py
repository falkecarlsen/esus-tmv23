"""
How to check that connection credentials are suitable for queries and writes from/into specified bucket.
"""
import os
from pprint import pprint

import influxdb_client
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
from influxdb_client.rest import ApiException
from influxdb_client import Point
import pandas as pd

from dotenv import load_dotenv
load_dotenv()


from datetime import datetime

from openpyxl.chart.series import Series

time_start = datetime.now()

# assume default port for InfluxDB
url = "http://localhost:8086"

token = os.getenv('INFLUXDB_TOKEN')
org = os.getenv('INFLUXDB_ORG')
bucket = os.getenv('INFLUXDB_DATABASE')


def check_connection(client):
    """Check that the InfluxDB is running."""
    print("> Checking connection ...", end=" ")
    client.api_client.call_api("/ping", "GET")
    print("ok")


def check_query(client):
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


def check_write(client):
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


def ingest(df: pd.DataFrame, mapping: pd.DataFrame, verbose=False):
    with InfluxDBClient(url=url, token=token, org=org) as client:
        check_connection(client)
        check_query(client)
        check_write(client)

    # do datetime conversion for timestamp to align with Point time-format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # drop Unnamed: 0 due to CSV serialisation
    df.drop(columns="Unnamed: 0")

    # replace spaces in source with underscores
    df["source"] = df["source"].str.replace(" ", "_")

    """
    point construction should follow:
    Measurement name: room
    Tag: room_id, sensor_type
    Field: value, unit (opt)
    example: room,room_id=1.213,sensor_type=temperature value=22.5,unit="C"
    """

    time_computation = datetime.now()
    print(f"Time pre-process dataset: {time_computation - time_start}")


    def dataframe_to_influxdb_points(dataframe: pd.DataFrame):
        for _, row in dataframe.iterrows():
            map_externallogid: Series = mapping.loc[int(row["externallogid"])]
            yield (
                Point(measurement_name="metric")
                .tag("source", row["source"])
                .tag("room_id", map_externallogid.location)
                .tag("sensor_type", map_externallogid.sensor_type)
                .field("unit", map_externallogid.unit)
                .field("value", row["value"])
                .time((row["timestamp"]))
            )


    with InfluxDBClient(url=url, token=token, org=org) as client:
        # write points
        step = 100_000
        i = 0
        print("Writing points")
        with client.write_api(write_options=WriteOptions(batch_size=10_000, flush_interval=1_000)) as con:
            for point in dataframe_to_influxdb_points(df):
                i += 1
                con.write(bucket, org, point)
                if verbose:
                    if i % step == 0:
                        print(".", end="")
                    if i % (step * 5) == 0:
                        print(f"\n{i / len(df):.3%} or {i}/{len(df)} points written, "
                              f"time elapsed: {datetime.now() - time_computation}, "
                              f"time left estimated: {(datetime.now() - time_computation) / i * (len(df) - i)}")

            time_write = datetime.now()
            print(f"\nTime to write {i} points: {time_write - time_computation}")

    print(f"Total time spent on dataset: {time_write - time_start}")