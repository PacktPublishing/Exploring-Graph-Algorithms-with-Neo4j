# -*- coding: utf-8 -*-
"""
Input data comes from http://dataratp.download.opendatasoft.com/RATP_GTFS_LINES.zip

The file contains one GTFS feed per line.

See https://gtfs.org/reference/ for documentation on the GTFS specification for public transport schedule.

This script downloads zip file from previous URL and prcess all files within it containing the METRO keyword only (we do not consider bus, tram and train networks).

A node is a station (uniquely identified by its name), with lat/lon property. Relationships are labelled SUBWAY and the line number together with average travel time are properties.

REQUIREMENTS: pandas, requests
"""
import os
import zipfile
import requests
import shutil
import pandas as pd

DATA_URL = "http://dataratp.download.opendatasoft.com/RATP_GTFS_LINES.zip"

NODE_LABEL = "Station"
RELATIONSHIP_TYPE = "SUBWAY"

TMP_PATH = "tmp/"
OUT_PATH = "out/"


def extract_zip(zipped_file, out_folder):
    """
    Extract zip file from `zipped_file` to `out_folder`
    """
    with zipfile.ZipFile(zipped_file, 'r') as zfile:
        zfile.extractall(path=out_folder)


def line_dir_avg(stop_times, direction):
    """Find trips with maximum number of stations
    (ignore those particular trips where some stations are dropped
    due to works or whatever)

    Then find the average time between two consecutive stations
    """
    trips_dir = stop_times [ stop_times.direction_id == direction]
    groups = trips_dir.groupby("trip_id")
    
    sizes = groups.size()
    max_size = sizes.max()
    t = stop_times [ stop_times.trip_id.isin(sizes[sizes == max_size].index) ]
    # ensure station order is correct
    t = t.sort_values(["trip_id", "stop_sequence"])
    # compute time difference between consecutive stations in a trip
    dt = t.groupby("trip_id").arrival_time_dt.diff().dt.total_seconds()
    # start, end and time diff between stations
    df = pd.DataFrame(
        {
            "start": t.stop_name,
            "end": t.groupby("trip_id").stop_name.shift(),
            "lat": t.stop_lat,
            "lon": t.stop_lon,
            "time": dt,
            "accessibility": "true",
        }
    )
    df = df.fillna(0)
    # compute average time between stations
    # also averages lat, lon and accessibility but those are constant values
    result = df.groupby(["start", "end"]).mean().reset_index()
    result["line"] = str(t.route_short_name.iloc[0])
    return result


def save_csv(results):
    """Save as CSV format compatible with Neo4j import tool
    """
    unique_stations = results.drop_duplicates(subset=["start", ])
    stations = pd.DataFrame({
        "name:ID": unique_stations.start,
        "lat:float": unique_stations.lat,
        "lon:float": unique_stations.lon,
        "accessibility:boolean": unique_stations.accessibility,
        ":LABEL": NODE_LABEL,
        }
    )
    stations.to_csv(os.path.join(OUT_PATH, "nodes_ALL.csv"),
                    index=False, sep=",")

    relations = pd.DataFrame({
        ":START_ID": results.start,
        "time:float": results.time,
        "line:string": results.line,
        ":END_ID": results.end,
        ":TYPE": RELATIONSHIP_TYPE,
        }
    )
    relations.to_csv(os.path.join(OUT_PATH, "relations_ALL.csv"),
                     index=False, sep=",")


def process_file(f):
    """
    For each line, process files into the corresponding zip file.
    """
    # read all needed files in the zip
    #with open(f) as z:
    with zipfile.ZipFile(f) as z:
        stops = pd.read_csv(z.open("stops.txt"))
        trips = pd.read_csv(z.open("trips.txt"))
        routes = pd.read_csv(z.open("routes.txt"))
        stop_times = pd.read_csv(z.open("stop_times.txt"))

    # concat everything into the stop_times DF
    stop_times = pd.merge(stop_times, stops, on="stop_id")
    stop_times = pd.merge(stop_times, trips, on="trip_id")
    stop_times = pd.merge(stop_times, routes, on="route_id")

    # convert arrival_time to datetime
    # ignore those rows where hour > 23 (GTFS spec)
    stop_times["arrival_time_dt"] = pd.to_datetime(
        stop_times.arrival_time, errors="coerce"
    )

    # drop columns contianing only NaN
    stop_times = stop_times.dropna(axis=1, how="all")
    # drop rows where time is NaT
    stop_times = stop_times [ stop_times.arrival_time_dt.notnull() ]

    # get aggregated data for direction 0 then 1
    # NB: maybe possible to do it in one go
    # but don't want to spend 80% of time on this 20% detail :)
    res = line_dir_avg(stop_times, direction=0)
    res2 = line_dir_avg(stop_times, direction=1)

    # concat results from direction 0 and 1
    result = pd.concat([res, res2], axis=0, ignore_index=True)
    result = result [ result.time > 0 ]    

    return result


def process_all_files(path):
    """Need to process all files and save results afterwards
    to drop duplicate stations
    """
    results = pd.DataFrame(columns=[
        "start", "end", "time", "line", "lat", "lon", "accessibility"
        ])
    for fn in os.listdir(path):
        if "METRO" not in fn:
            continue
        print("Processing file", fn)
        fnf = os.path.join(path, fn)
        res = process_file(fnf)
        results = results.append(res, ignore_index=True, sort=False)
    save_csv(results)


if __name__ == '__main__':
    # create tmp dir if not exists
    if not os.path.exists(TMP_PATH):
        os.mkdir(TMP_PATH)
    if not os.path.exists(OUT_PATH):
        os.mkdir(OUT_PATH)

    # download data file if not found locally
    F = "RATP.zip"
    if not os.path.exists(F):
        r = requests.get(DATA_URL)
        with open(F, 'wb') as f:
            f.write(r.content)
    # extract files from big RATP.zip file
    extract_zip(F, TMP_PATH)
    # resulting files are other zip files, one per line
    # containing GTFS feed
    process_all_files(TMP_PATH)

    # remove tmp dir
    shutil.rmtree(TMP_PATH)
