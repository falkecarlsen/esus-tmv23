import csv
import pandas as pd


# read dataframe from csv
df = pd.read_csv("output/test-large_20240115_080000_20240307_100157.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

unique_sources = []

for e in df["source"].unique():
    (src,) = df[df["source"] == e]["externallogid"].unique()
    unique_sources.append((src, e))

# sort unique sources based on textual name, second element in tuple
# unique_sources = list(sorted(unique_sources, key=lambda x: x[1]))
unique_sources = dict(unique_sources)

unique_sources = {
    6023: "Rum 1.215/TRU01_Rum Temp",
    6026: "Rum 1.215/VAV01",
    6027: "Rum 1.215/CO201_Rum ppm",
    6071: "/TM023_1_20_1103/SG01/Setting_solar_intensity_south_close",
    6074: "/TM023_1_20_1103/SG01/EK21_N",
    6126: "Rum 1.233/TRU01_Rum Temp",
    6129: "Rum 1.233/VAV01",
    6131: "/TM023_1_20_1103/SG01/Solar_panel_west",
    6132: "Rum 1.233/CO201_Rum ppm",
    6136: "/TM023_1_20_1103/SG01/Solar_panel_south",
    6220: "Rum 1.229/TRU01_Rum Temp",
    6223: "Rum 1.229/VAV01",
    6225: "Rum 1.229/CO201_Rum ppm",
    6234: "/TM023_1_20_1103/SG01/Frigivet",
    6278: "/TM023_1_20_1103/SG01/Setting_solar_intensity_south_open",
    6287: "Rum 1.231/TRU01_Rum Temp",
    6289: "Rum 1.231/VAV01",
    6291: "Rum 1.231/CO201_Rum ppm",
    6322: "/TM023_1_20_1103/SG01/Setting_solar_intensity_east_close",
    6327: "/TM023_1_20_1103/SG01/Setting_upper_limit_wind",
    6336: "Rum 1.217/TRU01_Rum Temp",
    6339: "Rum 1.217/VAV01",
    6340: "Rum 1.217/CO201_Rum ppm",
    6369: "/TM023_1_20_1103/SG01/Setting_solar_intensity_east_open",
    6372: "/TM023_1_20_1103/SG01/Setting_release_panels_under_limit",
    6397: "Rum 1.213/TRU01_Rum Temp",
    6399: "Rum 1.213/VAV01",
    6401: "Rum 1.213/CO201_Rum ppm",
    6418: "/TM023_1_20_1103/SG01/EK20_N",
    6426: "/TM023_1_20_1103/SG01/Solar_panel_east",
    6428: "/TM023_1_20_1103/SG01/Wind_velocity",
    6432: "/TM023_1_20_1103/SG01/Power_switch_south_east",
    233018: "Rum 1.213/PIR_activity_1.213",
    233020: "Rum 1.215/PIR_activity_1.215",
    233022: "Rum 1.217/PIR_activity_1.217",
    233027: "Rum 1.229/PIR_activity_1.229",
    233028: "Rum 1.231/PIR_activity_1.231",
    233030: "Rum 1.233/PIR_activity_1.233",
    249709: "Energi_heating_north_east_VA01",
    249780: "Volume_accumulated_north_east_VA01",
    249781: "Effect_north_east_VA01",
    249782: "Flow_north_east_VA01",
    249783: "Supply_temperature_north_east_VA01",
    249784: "Return_temperature_north_east_VA01",
    249785: "Operation_hours_accumulated_north_east_VA01",
    696836: "Rum 1.213/Vindue_1.213",
    730983: "Rum 1.215/CO2_1.215",
    730984: "Rum 1.215/Room_temperature_1.215",
    730985: "Rum 1.215/Temperature_heating_setpoint_1.215",
    730986: "Rum 1.215/Temperature_cooling_setpoint_1.215",
    730987: "Rum 1.215/Lux_meter_1.215",
    740547: "Rum 1.215/LY01_1.215",
    740548: "Rum 1.215/LN01_1.215",
    752337: "Rum 1.233/Temperature_heating_setpoint_1.233",
    752338: "Rum 1.233/Temperature_cooling_setpoint_1.233",
    752339: "Rum 1.231/Temperature_heating_setpoint_1.231",
    752340: "Rum 1.231/Temperature_cooling_setpoint_1.231",
    752341: "Rum 1.229/Temperature_heating_setpoint_1.229",
    752342: "Rum 1.229/Temperature_cooling_setpoint_1.229",
    752347: "Rum 1.217/Temperature_heating_setpoint_1.217",
    752348: "Rum 1.217/Temperature_cooling_setpoint_1.217",
    752349: "Rum 1.213/Temperature_heating_setpoint_1.213",
    752350: "Rum 1.213/Temperature_cooling_setpoint_1.213",
    752474: "Rum 1.213/VAV02",
    752475: "Rum 1.213/Offset_STR",
    752476: "Rum 1.213/Lux_meter",
    752477: "Rum 1.213/Light_niveau",
    752478: "Rum 1.213/Light_supply",
    752479: "Rum 1.213/Radiator_valve",
    752480: "Rum 1.213/Rum_Temp_Dag",
    752481: "Rum 1.213/Temp_SP_dag",
    752482: "Rum 1.213/Temp_SP_standby",
    752483: "Rum 1.213/Temp_SP_night",
    752484: "Rum 1.213/Efterløbstid_spjæld",
    752485: "Rum 1.213/Lux_SP",
    752486: "Rum 1.213/Lux_efterløbstid",
    752487: "Rum 1.213/Ventilation_status",
    752488: "Rum 1.213/Rumpanel_lys",
    752489: "Rum 1.213/Afbryder_loft_lys",
    752490: "Rum 1.215/Afbryder_loft_lys",
    752491: "Rum 1.215/Rumpanel_lys",
    752492: "Rum 1.215/Offset_STR",
    752493: "Rum 1.215/Ventilation_status",
    752494: "Rum 1.215/Radiator_valve",
    752495: "Rum 1.215/VAV02",
    752496: "Rum 1.215/Rum_Temp_Dag",
    752497: "Rum 1.215/Temp_SP_dag",
    752498: "Rum 1.215/Temp_SP_standby",
    752499: "Rum 1.215/Temp_SP_night",
    752500: "Rum 1.215/Efterløbstid_spjæld",
    752501: "Rum 1.215/Lux_SP",
    752502: "Rum 1.215/Lux_efterløbstid",
    752503: "Rum 1.217/VAV02",
    752504: "Rum 1.217/Offset_STR",
    752505: "Rum 1.217/Lux_meter",
    752506: "Rum 1.217/Light_niveau",
    752507: "Rum 1.217/Light_supply",
    752508: "Rum 1.217/Radiator_valve",
    752509: "Rum 1.217/Rum_Temp_Dag",
    752510: "Rum 1.217/Temp_SP_dag",
    752511: "Rum 1.217/Temp_SP_standby",
    752512: "Rum 1.217/Temp_SP_night",
    752513: "Rum 1.217/Efterløbstid_spjæld",
    752514: "Rum 1.217/Lux_SP",
    752515: "Rum 1.217/Lux_efterløbstid",
    752516: "Rum 1.217/Ventilation_status",
    752517: "Rum 1.217/Rumpanel_lys",
    752518: "Rum 1.217/Afbryder_loft_lys",
    752519: "Rum 1.229/VAV02",
    752520: "Rum 1.229/Offset_STR",
    752521: "Rum 1.229/Lux_meter",
    752522: "Rum 1.229/Light_niveau",
    752523: "Rum 1.229/Light_supply",
    752524: "Rum 1.229/Radiator_valve",
    752525: "Rum 1.229/Rum_Temp_Dag",
    752526: "Rum 1.229/Temp_SP_dag",
    752527: "Rum 1.229/Temp_SP_standby",
    752528: "Rum 1.229/Temp_SP_night",
    752529: "Rum 1.229/Efterløbstid_spjæld",
    752530: "Rum 1.229/Lux_SP",
    752531: "Rum 1.229/Lux_efterløbstid",
    752532: "Rum 1.229/Ventilation_status",
    752533: "Rum 1.229/Rumpanel_lys",
    752534: "Rum 1.229/Afbryder_loft_lys",
    752535: "Rum 1.231/VAV02",
    752536: "Rum 1.231/Offset_STR",
    752537: "Rum 1.231/Lux_meter",
    752538: "Rum 1.231/Light_niveau",
    752539: "Rum 1.231/Light_supply",
    752540: "Rum 1.231/Radiator_valve",
    752541: "Rum 1.231/Rum_Temp_Dag",
    752542: "Rum 1.231/Temp_SP_dag",
    752543: "Rum 1.231/Temp_SP_standby",
    752544: "Rum 1.231/Temp_SP_night",
    752545: "Rum 1.231/Efterløbstid_spjæld",
    752546: "Rum 1.231/Lux_SP",
    752547: "Rum 1.231/Lux_efterløbstid",
    752548: "Rum 1.231/Ventilation_status",
    752549: "Rum 1.231/Rumpanel_lys",
    752550: "Rum 1.231/Afbryder_loft_lys",
    752551: "Rum 1.233/VAV02",
    752552: "Rum 1.233/Offset_STR",
    752553: "Rum 1.233/Lux_meter",
    752554: "Rum 1.233/Light_niveau",
    752555: "Rum 1.233/Light_supply",
    752556: "Rum 1.233/Radiator_valve",
    752557: "Rum 1.233/Rum_Temp_Dag",
    752558: "Rum 1.233/Temp_SP_dag",
    752559: "Rum 1.233/Temp_SP_standby",
    752560: "Rum 1.233/Temp_SP_night",
    752561: "Rum 1.233/Efterløbstid_spjæld",
    752562: "Rum 1.233/Lux_SP",
    752563: "Rum 1.233/Lux_efterløbstid",
    752564: "Rum 1.233/Ventilation_status",
    752565: "Rum 1.233/Rumpanel_lys",
    752566: "Rum 1.233/Afbryder_loft_lys",
    753498: "Rum 1.213/CO2_1.213",
    753499: "Rum 1.213/TRU01_1.213",
    753500: "Rum 1.217/TRU01_1.217",
    753501: "Rum 1.217/CO2_1.217",
    753502: "Rum 1.229/TRU01_1.229",
    753503: "Rum 1.229/CO2_1.229",
    753504: "Rum 1.231/TRU01_1.231",
    753505: "Rum 1.231/CO2_1.231",
    753506: "Rum 1.233/TRU01_1.233",
    753507: "Rum 1.233/CO2_1.233",
}


df.drop(columns=["Unnamed: 0", "source"], inplace=True)

df.to_csv("output/test-large-stripped_20240115_080000_20240307_100157.csv", index=False)
pd.DataFrame(unique_sources.items(), columns=["externallogid", "source"]).to_csv("output/mapping.csv")