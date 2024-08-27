# ESUS-TMV23
Work-in-progress project on saving heating/cooling costs in the AAU TMV23 building.

The goal is to synthesise control strategies (Imran papers) for a subset of the TMV23 building and apply this through manipulating temperature-setpoints and actuating external window shades.
Presumed savings are among others from utilising weather forecasts and preemptively lowering shades in morning on sunny days to save cooling costs on south-facing rooms. Currently, these shades are not tied into the main BMS and are only controlled by human interaction.


# Limitations
- Shades are not digitally controllable (LK svagstrømstryk installed) without custom hardware. 
- Main BMS (Schneider Electrics) is run on old bus which cannot handle much bandwidth. Limited to update frequency of ~5mins.


# Progress
- Refactor API to higher standard; error-handling, proper export of time-series, and speedup (python wrapper onto TMV23-internal BMS system) 
    - Have enabled high-frequency updates from API into CSV/pd.dataframe
- Setup InfluxDB container for ingesting datasource for visualising and identifying interesting points in time for acting on the heating system.
- Preliminary work into developing custom-panel interfacing with window-shade control switch.
    - Mechanical actuation parts, physical design, user-override mechanism conceptualised.
    - Perhaps possible to electrically (opposed to physically actuating) actuate on shades as they are max 24VDC (https://www.lavprisel.dk/lk-fuga-svagstroemstryk-4-slutte-1-modul-hvid-43923). Might be hard to convince Building Support.
- Currently stuck due to time-limitations.
