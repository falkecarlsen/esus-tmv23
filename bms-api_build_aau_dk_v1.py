# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 16:10:44 2022

@author: GQ05XY
"""
from datetime import datetime
from io import StringIO

import requests
import pandas as pd
import datetime as dt
import json
import os
import time

DEBUG_API = False

time_start = datetime.now()

# name for export
RUN = "TMV23"

pd.set_option("display.max_colwidth", None)
###############################
# API endpoints for the DB server
TRENDDATA_NAME = "https://bms-api.build.aau.dk/api/v1/trenddata"
METADATA_NAME = "https://bms-api.build.aau.dk/api/v1/metadata"

# Set the username for the DB server (if you do not have one ask Simon)
USERNAME = ""
# Set the password for the DB server (if you do not have one ask Simon)
PASSWORD = ""

if not USERNAME:
    USERNAME = os.environ.get("SE_API_USERNAME")
if not PASSWORD:
    PASSWORD = os.environ.get("SE_API_PASSWORD")

# start time for the data extraction

def fetch(
    url: str, params: dict, username: str, password: str
) -> requests.Response | None:
    status_code = 0
    tries = 0
    while status_code != requests.status_codes.codes.OK:
        payload = requests.get(url, params=params, auth=(username, password))
        status_code = payload.status_code

        if tries > 3:
            print(
                f"Error in fetching from API. Status code: {status_code}. Tried {tries} times. Bailing out."
            )
            return None

        elif status_code != 200:
            print(
                f"Error in fetching from API. Status code: {status_code}. Retrying in {5 * tries} seconds"
            )
            time.sleep(5 * tries)
            tries += 1

        else:
            return payload

#start = dt.datetime(2024, 1, 15, 8)
end = dt.datetime.now() - dt.timedelta(hours=1)
start = end - dt.timedelta(days=1)

# make datetimes and ensure ordering and duration is greater than 15 minutes
if start > end:
    raise ValueError("Start time is after end time")
if (end - start).total_seconds() < 15 * 60:
    raise ValueError("Duration is less than 15 minutes")

###############################
# Set the path for where the data file should be saved to as a string, e.g. 'C:/Users/GQ05XY/Aalborg Universitet/SATO - General/TMV23/Data_from_BMS/Data_dumb_from_DB'
save_location = "output"
# Set the name of the data file as a string, e.g. 'test'
# make timestamp from start and end datetimes in short readable format for filename
save_file_name = (
    f"{RUN}_{start.strftime('%Y%m%d_%H%M%S')}_{end.strftime('%Y%m%d_%H%M%S')}"
)
# Set the type that the data should be saved as, as a string
# Option 1 : 'xlsx'
# Option 2 : 'csv'
# Option 3 : 'json'
# Option 4 : feather½
save_file_type = "csv"
# Should the file be human readable (larger file size) or only machine readable (smaller file size), only relevant for JSON format
# Option 1 : Human readable, insert 1 tabs
# Option 2 : Only machine readable, insert 2 tabs
readability = 2


###############################
# Identifier for the data to be extracted
# 1 is for setting the externallogid directly
# 2 is for setting a single input using the full source string
# 3 is for using a logmap for pulling any number of variables
identifier = 3

###############################
# if identifier is 1, externallogid is used (for few inputs)
# Fill in this list with values, e.g. [4269] or [1,7,9,1152,4520]
externallogid_list = [[4697]]

###############################
# if identifier is 2, the manual input of source is used (for single input)
# Fill in the path in the BMS system to the variable as a string, e.g. '/TM023_0_22_1125/Intake_ventilation_unit_on-off'
source_string = "/TM023_0_20_1128/Lon/Net/FO01_QMV01/Kamstrup_HEAT_KMP_TAC/Node Object [0]/DH_whole_building_power"


###############################
# if identifier is 3, the source_df is pulled from the excel sheet logmap (for many inputs)
# Fill in the path to the logmap as a string, e.g. 'C:/Users/GQ05XY/Aalborg Universitet/SATO - General/TMV23/Data_from_BMS/Log_map_TMV23.xlsx'
source_logmap = "Log_map_TMV23_2023_12_30_til_Falke.xlsx"
# Fill in the name of the sheet containing the logmap as a string, e.g. 'log_map'
logmap_sheet = "log_map"
# Fill in the columns that should be imported from the logmap file as a string, e.g. 'A:D'
logmap_columns = "A:D"
# Fill in the name of the column containing the location of the log variable as a string, e.g. 'Log_variable_location'
logmap_var_loc = "Log_variable_location"
# Fill in the name of the column containing the name of the log variable as a string, e.g. 'Logged_variable_name'
logmap_var_name = "Logged_variable_name"


# print configuration status line
print(
    f"Configuration: {save_file_type=}, {save_location=}, {RUN=}, {start=}, {end=}, {identifier=}, {readability=}"
)

#########################
##     Script below    ##
##     Do not touch!   ##
#########################


if identifier == 1:
    externallogid = (
        externallogid_list  # create the list of externallogid from externallogid_list
    )


elif identifier == 2:
    source = source_string  # create the source from source_string
    trend_meta = fetch(
        url=METADATA_NAME, params={}, username=USERNAME, password=PASSWORD
    )
    trend_meta_text = trend_meta.text
    trend_meta_df = pd.read_json(
        trend_meta_text, orient="records"
    )  # extract the trend_meta data from the DB
    externallogid = trend_meta_df.externallogid[
        source == trend_meta_df.source
    ].tolist()  # find the correct externallogid based on the source and the trend_meta data

    del source


elif identifier == 3:
    # create the source from the logmap, with the specified naming and location
    source_df = pd.read_excel(
        io=source_logmap, sheet_name=logmap_sheet, header=0, usecols=logmap_columns
    )
    source = []
    for i in range(0, len(source_df)):
        temp = "/".join(
            [str(source_df[logmap_var_loc][i]), str(source_df[logmap_var_name][i])]
        )  # create a full path with variable name for each variable
        source.append(
            temp
        )  # append the full path of each variable to this list, to get a full list of variable paths
    trend_meta = fetch(
        url=METADATA_NAME, params={}, username=USERNAME, password=PASSWORD
    )

    trend_meta_df = pd.read_json(StringIO(trend_meta.text), orient="records")
    externallogid = []
    temp_externallogid = []
    for i in range(0, len(source_df)):
        externallogid.append(
            trend_meta_df.externallogid[source[i] == trend_meta_df.source].tolist()
        )  # create a list of all valid externallogid
        temp_externallogid.append(
            trend_meta_df.externallogid[source[i] == trend_meta_df.source].tolist()
        )  # create a list of all externallogid
        if externallogid[-1] == []:
            print(
                " ".join([f"source_df {source_df.index[i]=}, {source_df[logmap_var_name][i]=} failed"])
            )  # Print the index corresponding to source_df for each variable, which did not have a valid externallogid
            externallogid.pop()  # remove the invalid externallogid from the list of all valid externallogid
    source_df_insert_point = len(source_df.columns)
    source_df.insert(
        source_df_insert_point, "externallogid", temp_externallogid
    )  # append the list of all externallogid to the source_df for an easy overview of which externallogid are missing
    print("Meta data done")

# extract the trend_data
PARAMS = {"starttime": start, "endtime": end, "externallogid": externallogid}
#trend_data = fetch(TRENDDATA_NAME, params=PARAMS, username=USERNAME, password=PASSWORD)
trend_data = None
# init empty json
tmp_df = pd.DataFrame()
trend_meta_df = None
print(f"Extracting data, {len(externallogid)} externallogid(s) repr. by '.'")
if DEBUG_API or trend_data is None:
    failed_external_logid = []
    for i in range(1, len(externallogid)):
        PARAMS["externallogid"] = externallogid[i]
        trend_data = requests.get(
            TRENDDATA_NAME, params=PARAMS, auth=(USERNAME, PASSWORD)
        )
        if trend_data.status_code != 200:
            failed_external_logid.append(externallogid[i])
            print("x", end="")
            time.sleep(1)
        else:
            new_df = pd.read_json(StringIO(trend_data.text), orient="records")
            #print(new_df.size, end=" ")
            tmp_df = pd.concat([tmp_df, new_df])
            print(".", end="" if i % 10 != 0 else f" {len(externallogid) - i} left \n")

    print(end="\n")
    trend_data_df = tmp_df
    if len(failed_external_logid) > 0:
        print(f"Failed externallogid(s): {failed_external_logid}")
else:
    # extract the trend_data for each externallogid in the timespan between starttime and endtime
    trend_data_df = pd.read_json(trend_data.text, orient="records")



print("Data extracted")

if identifier == 3:
    source_df["externallogid"] = source_df["externallogid"].explode()

    dfs = []
    for id_, id_df in trend_data_df.groupby("externallogid"):
        temp_source = (
            source_df[source_df["externallogid"] == id_]["Log_variable_location"]
        ).tolist()
        temp_name = (
            source_df[source_df["externallogid"] == id_]["Logged_variable_name"]
        ).tolist()

        temp_source_name = "/".join(temp_source + temp_name)

        temp_s = pd.Series([temp_source_name] * len(id_df))
        temp_df = pd.concat(
            [id_df.reset_index(drop=True), temp_s.reset_index(drop=True)],
            ignore_index=True,
            axis=1,
        )

        dfs.append(temp_df)

    trend_data_df = pd.concat(dfs, axis=0, ignore_index=True)
    col_names = ["externallogid", "timestamp", "timestamp_tzinfo", "value", "source"]
    trend_data_df.columns = col_names
    source_column = trend_data_df.pop("source")
    trend_data_df.insert(1, "source", source_column)
    print("Source added")


# save the file with the desired name, location and filetype
save_file = "".join([save_location, "/", save_file_name, ".", save_file_type])

if save_file_type == "xlsx":
    trend_data_df.to_excel(save_file)
elif save_file_type == "csv":
    trend_data_df.to_csv(save_file)
elif save_file_type == "feather":
    trend_data_df.to_feather(save_file)

elif save_file_type == "json":
    readability = 0
    if readability == 1:
        indentation = 4
    trend_data_output = trend_data_df.to_json(orient="table", indent=indentation)
    with open(save_file, "w", encoding="utf-8") as f:
        json.dump(trend_data_output, f, ensure_ascii=False, indent=indentation)


time_end = datetime.now()
print("Output saved to: ", save_file)
print(f"Time to run: {time_end - time_start}")