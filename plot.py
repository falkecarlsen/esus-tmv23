"""
Plot a graph from a dataframe, read from csv
"""
from pprint import pprint

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# sns.set_context("paper")


"""
logs of interest:
- Solar panel east
- TRU01_Rum Temp
- CO201_Rum ppm
- Room_temperature_1.215
- Temperature_heating_setpoint_1.215 (room number abstraction)
- Temperature_cooling_setpoint_1.215 (room number abstraction)
- Radiator_valve
- 
"""
csv_file = "output/test-large-stripped_20240115_080000_20240307_100157.csv"

# pd.set_option("figure.max_open_warning", False)
# set larger size
sns.set_theme(context="paper", style="whitegrid", palette="deep", font_scale=0.8)

# Read the csv file
df = pd.read_csv(csv_file)
df["timestamp"] = pd.to_datetime(df["timestamp"])
# get mappings
id_map = (
    pd.read_csv("output/mapping.csv").set_index("externallogid").drop(columns="Unnamed: 0").to_dict("dict")["source"]
)
# make reverse of id_map dict
name_map = dict(map(reversed, id_map.items()))

pprint(id_map)

df = df[df["timestamp"] > df["timestamp"].max() - pd.Timedelta(days=7)]

# pick only the sources we want to plot (rum temperatur, co2, solar panel, room temperature, heating setpoint, cooling setpoint, radiator valve)
temp_ids = [
    "Rum 1.215/TRU01_Rum Temp",
    "Rum 1.233/TRU01_Rum Temp",
    "Rum 1.229/TRU01_Rum Temp",
    "Rum 1.231/TRU01_Rum Temp",
    "Rum 1.217/TRU01_Rum Temp",
    "Rum 1.213/TRU01_Rum Temp",
]

df_temp = df[df["externallogid"].isin([name_map[e] for e in temp_ids])]
df_co2 = df[
    df["externallogid"].isin(
        [
            name_map[e]
            for e in [
                "Rum 1.215/CO201_Rum ppm",
                "Rum 1.233/CO201_Rum ppm",
                "Rum 1.229/CO201_Rum ppm",
                "Rum 1.231/CO201_Rum ppm",
                "Rum 1.217/CO201_Rum ppm",
                "Rum 1.213/CO201_Rum ppm",
                "Rum 1.215/CO2_1.215",
                "Rum 1.213/CO2_1.213",
                "Rum 1.217/CO2_1.217",
                "Rum 1.229/CO2_1.229",
                "Rum 1.231/CO2_1.231",
                "Rum 1.233/CO2_1.233",
            ]
        ]
    )
]


sns.relplot(df_temp, kind="line", x="timestamp", y="value", hue="externallogid")
sns.relplot(df_co2, kind="line", x="timestamp", y="value", hue="externallogid")
plt.xticks(rotation=45)
plt.show()
exit()

for name, group in df:
    print(name)
    # print(group)
    continue
    group.plot(x="timestamp", y=["value"], title=f"source {name}")
    # make title of plot in a small font
    plt.title(f"source {name}", fontsize=8)
    plt.show()
    print("\n")

exit()

df = df.set_index("timestamp")

# rename value to temp
# df = df.rename(columns={"value": "temp"})

# drop some columns
df = df.drop(columns="Unnamed: 0")
df = df.drop(columns="timestamp_tzinfo")
# df = df.drop(columns="externallogid")

# draw each DIFFERENT externallogid as a separate graph


# resize df to have last 7 days by timedelta
# df = df[df.index > pd.to_datetime("2024-01-29 00:00:00")]

# plot the data
fig, ax = plt.subplots()
df.plot(ax=ax)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
plt.show()
