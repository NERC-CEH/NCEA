# -*- coding: utf-8 -*-
"""
Functions to query and fetch data from the following sources:

EA water quality
EA ecology and fish data
EA additional gauging stations
Riverfly survey
FreshwaterWatch
GRTS monitoring locations

"""
import paths

import urllib.request
import json
import pandas as pd
from datetime import datetime


# *** EA water quality ********************************************************
"""
Data for EA water quality (WQ) is fetched from an API.

"""
def get_WQ_stations():
    """
    Fetch all available stations for EA water quality data and return in format
    for station register.

    """
    limit = 500
    offset = 0

    stations = []
    finished = False
    while finished is False:
        # Query string
        query = "_limit=%s&_offset=%s" % (limit, offset)
        # Full URL
        url = "%s?%s" % (paths.WQ_API_URL, query)

        with urllib.request.urlopen(url) as response:
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available stations
            if len(data["items"]) < limit:
                finished = True

            for station_info in data["items"]:
                station = {
                    "id": station_info["notation"],
                    "name": station_info["label"],
                    "network": "EA_WQ",
                    "easting": station_info["easting"],
                    "northing": station_info["northing"],
                }
                stations.append(station)

        offset += limit

    return pd.DataFrame(stations)


def get_WQ_determinands_for_station(station_id):
    """
    Fetch all determinands for an EA water quality station

    """
    url = "%s/%s/determinands.json" % (paths.WQ_API_URL, station_id)

    determinands = None
    with urllib.request.urlopen(url) as response:
        data = json.load(response)
        determinands = pd.DataFrame(data['items'])

    return determinands


def get_WQ_measurements_for_station(station_id, determinand_id=None):
    """
    Fetch all measurements for an EA water quality station. Optionally filter
    by determinand.

    """
    limit = 500
    offset = 0

    base_query = ""
    if determinand_id is not None:
        base_query = "determinand=%s&" % determinand_id

    measurements = []
    finished = False
    while finished is False:
        # Query string
        query = "%s_limit=%s&_offset=%s" % (base_query, limit, offset)
        # Full URL
        url = "%s/%s/measurements.json?%s" % (paths.WQ_API_URL, station_id, query)

        with urllib.request.urlopen(url) as response:
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available measurements
            if len(data["items"]) < limit:
                finished = True

            for item in data['items']:
                measurement = {
                    "determinand_id": item["determinand"]["notation"],
                    "value": item["result"],
                    "date_time": datetime.strptime(
                        item["sample"]["sampleDateTime"],
                        "%Y-%m-%dT%H:%M:%S")
                }
                measurements.append(measurement)

        offset += limit

    return pd.DataFrame(measurements)


# *** EA ecology and fish data ************************************************
"""
Data for EA ecology and fish data (EFD) is fetched from an API.

"""
EFD_BASE_URL = "https://environment.data.gov.uk/ecology/api/v1/"

def get_EFD_stations():
    """
    Fetch all available stations for EA ecology and fish data and return in
    format for station register.

    """
    limit = 500
    offset = 0

    stations = []
    finished = False
    while finished is False:
        # Query string
        query = "take=%s&skip=%s&mode=props" % (limit, offset)
        # Full URL
        url = "%ssites?%s" % (EFD_BASE_URL, query)

        with urllib.request.urlopen(url) as response:
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available stations
            if len(data) < limit:
                finished = True

            for station_info in data:
                if station_info.get("easting") is None:
                    # Not all items have coordinates provided. Ignore these.
                    continue

                s_type = station_info["type"].split("/")[-1]
                if s_type == "FishFreshwaterSite":
                    network = "EA_fish"
                elif s_type == "BiosysFreshwaterSite":
                    network = "EA_biosys"
                else:
                    print("Unknown station type: %s" % s_type)
                    continue

                station = {
                    "id": station_info["local_id"],
                    "name": station_info["label"],
                    "network": network,
                    "easting": station_info["easting"],
                    "northing": station_info["northing"],
                }
                stations.append(station)

        offset += limit

    return pd.DataFrame(stations)


# *** EA additional gauging stations ******************************************
"""
Data for EA additional gauging stations (GS) is fetched from a CSV. These are
station not included in NRFA.

"""



stations = get_EFD_stations()
import pdb; pdb.set_trace()


stations.to_csv(paths.STATION_REGISTER, index=False)

measures = get_WQ_measurements_for_station("AN-BASSING", "0111")
