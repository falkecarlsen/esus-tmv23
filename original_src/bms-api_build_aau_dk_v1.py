# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 16:10:44 2022

@author: GQ05XY
"""

import requests
import pandas as pd
import datetime as dt
import json
import csv
import os

pd.set_option("display.max_colwidth", None)
###############################
# API endpoints for the DB server
TRENDDATA_NAME = "https://bms-api.build.aau.dk/api/v1/trenddata"
METADATA_NAME = "https://bms-api.build.aau.dk/api/v1/metadata"

# Set the username for the DB server (if you do not have one ask Simon)
username = ""
# Set the password for the DB server (if you do not have one ask Simon)
password = ""


###############################
# Start and endtime for the dataextraction
start_year = 2023
start_month = 2
start_day = 20
start_hour = 8
end_year = 2023
end_month = 2
end_day = 20
end_hour = 9

###############################
# Set the path for where the data file should be saved to as a string, e.g. 'C:/Users/GQ05XY/Aalborg Universitet/SATO - General/TMV23/Data_from_BMS/Data_dumb_from_DB'
save_location = ""
# Set the name of the data file as a string, e.g. 'test'
save_file_name = ""
# Set the type that the data should be saved as, as a string
# Option 1 : 'xlsx'
# Option 2 : 'csv'
# Option 3 : 'json'
# Option 4 : feather
save_file_type = ""
# Should the file be human readable (larger file size) or only machine readable (smaller file size), only relevant for JSON format
# Option 1 : Human readable, insert 1
# Option 2 : Only machine readable, insert 2
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
source_logmap = "C:/Users/GQ05XY/Aalborg Universitet/SATO - General/TMV23/Data_from_BMS/Log_map_TMV23_v5.xlsx"
# Fill in the name of the sheet containing the logmap as a string, e.g. 'log_map'
logmap_sheet = "log_map"
# Fill in the columns that should be imported from the logmap file as a string, e.g. 'A:D'
logmap_columns = "A:D"
# Fill in the name of the column containing the location of the log variable as a string, e.g. 'Log_variable_location'
logmap_var_loc = "Log_variable_location"
# Fill in the name of the column containing the name of the log variable as a string, e.g. 'Logged_variable_name'
logmap_var_name = "Logged_variable_name"

#########################################################################################################################################################################
##                                                                  Script below                                                                                       ##
##                                                                  Do not touch!                                                                                      ##
#########################################################################################################################################################################
# create the list of externallogid

if identifier == 1:
    externallogid = (
        externallogid_list  # create the list of externallogid from externallogid_list
    )


elif identifier == 2:
    source = source_string  # create the source from source_string
    trend_meta = requests.get(url=METADATA_NAME, auth=(username, password))
    trend_meta_text = trend_meta.text
    trend_meta_df = pd.read_json(
        trend_meta_text, orient="records"
    )  # extract the trend_meta data from the DB
    externallogid = trend_meta_df.externallogid[
        source == trend_meta_df.source
    ].tolist()  # find the correct externallogid based on the source and the trend_meta data

    del source


elif identifier == 3:
    source_df = pd.read_excel(
        io=source_logmap, sheet_name=logmap_sheet, header=0, usecols=logmap_columns
    )  # create the source from the logmap, with the specified naming and location
    source = []
    for i in range(0, len(source_df)):
        temp = "/".join(
            [str(source_df[logmap_var_loc][i]), str(source_df[logmap_var_name][i])]
        )  # create a full path with variable name for each variable
        source.append(
            temp
        )  # append the full path of each variable to this list, to get a full list of variable paths
    trend_meta = requests.get(
        url=METADATA_NAME, auth=(username, password)
    )  # extract the trend_meta data as a dataframe
    trend_meta_text = trend_meta.text
    trend_meta_df = pd.read_json(trend_meta_text, orient="records")
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
                " ".join(["source_df index", str(source_df.index[i]), "failed"])
            )  # Print the index corresponding to source_df for each variable, which did not have a valid externallogid
            externallogid.pop()  # remove the invalid externallogid from the list of all valid externallogid
    source_df_insert_point = len(source_df.columns)
    source_df.insert(
        source_df_insert_point, "externallogid", temp_externallogid
    )  # append the list of all externallogid to the source_df for an easy overview of which externallogid are missing
    print("Meta data done")
    del temp
    del temp_externallogid
    del source
    del i


# extract the trend_data
starttime = dt.datetime(
    start_year, start_month, start_day, start_hour
)  # create the startime indicator for the data extraction
endtime = dt.datetime(
    end_year, end_month, end_day, end_hour
)  # create the endtime indicator for the data extraction
PARAMS = {"starttime": starttime, "endtime": endtime, "externallogid": externallogid}

trend_data = requests.get(
    TRENDDATA_NAME, params=PARAMS, auth=(username, password)
)  # extract the trend_data for each externallogid in the timespan between starttime and endtime
trend_data_text = trend_data.text
trend_data_df = pd.read_json(trend_data_text, orient="records")

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
    if readability == 1:
        indentation = 4
    elif readability == 2:
        indentation = 0
    trend_data_output = trend_data_df.to_json(orient="table", indent=indentation)
    with open(save_file, "w", encoding="utf-8") as f:
        json.dump(trend_data_output, f, ensure_ascii=False, indent=indentation)
    del indentation
    del f


# clear the variables not needed further
del start_year
del start_month
del start_day
del start_hour
del end_year
del end_month
del end_day
del end_hour
del externallogid_list
del source_string
del source_logmap
del logmap_sheet
del logmap_columns
del logmap_var_loc
del logmap_var_name
del readability
del save_file_name
del save_file_type
del save_location
del trend_data
del trend_data_text
if identifier > 1:
    del trend_meta
    del trend_meta_text


del identifier
