# -*- coding: utf-8 -*-
"""
Functions to create the site and data availability metadata files for the
following networks:

    - EA water quality (EA_WQ)
    - EA Macroinvertebrates (EA_INV)
    - EA Macrophyte (EA_MACP)
    - EA Diatom (EA_DIAT)
    - EA fish count data (EA_FISH)
    - Riverfly survey (RS)
    - FreshwaterWatch (FWW)
    - National river flow archive (NRFA)

There are 3 CSV metadata files:
    - site_register      - Row for data on each site
    - data_type_register - Row for data on each data type
    - data_availability  - Row for data on available data for each site / data
                           type

One of these for each network.
These can then be converted to JSON files. There are two types of JSON file:

    - network_info      - Overall information on the network plus site wide
                          stats for each data type
    - data_availability - Combines all three CSVs in one JSON, is split by IHU
                          area.

The functions to run are listed at the bottom. These only need to be run once.
Some networks read data from downloaded files:
    EA_INV, EA_MACP, EA_DIAT, EA_FISH, RS, FWW

Others read data from an API:
    EA_WQ

Others from a combination of both:
    NRFA

The API networks can be run whenever update (although EA_WQ will take a long
time). The networks reading from files require the raw file to be manually
updated before re-running.

"""
import paths
import config

import urllib.request
import os
import json
import pandas as pd
import numpy as np
import geopandas as gpd

from geopandas.tools import sjoin
from datetime import datetime
from pyproj import Transformer


def make_network_dict(network_id=None, network_name=None, network_desc=None,
                      folder=None, shape=None, access=None, updates=None,
                      website=None, dtype_ids=[]):
    """
    Create dtype_register dictionary. Equates to a row in the metadata file.

    """
    return {
        "NETWORK_ID": network_id,
        "NETWORK_NAME": network_name,
        "NETWORK_DESC": network_desc,
        "FOLDER": folder,
        "SHAPE": shape,
        "ACCESS": access,
        "UPDATES": updates,
        "WEBSITE": website,
        "DTYPE_IDS": dtype_ids,
    }


def make_site_dict(site_id=None, site_name=None, network_id=None, lat=None,
                   long=None, alt_coords=None):
    """
    Create site_register dictionary. Equates to a row in the metadata file.

    """
    return {
        "SITE_ID": site_id,
        "SITE_NAME": site_name,
        "NETWORK_ID": network_id,
        "LATITUDE": lat,
        "LONGITUDE": long,
        "ALT_COORDS": alt_coords,
    }


def make_dtype_dict(dtype_id=None, dtype_name=None, network_id=None,
                    dtype_desc=None, units=None, value_min=None,
                    percentile_20=None, percentile_40=None, percentile_60=None,
                    percentile_80=None, value_max=None, value_mean=None,
                    value_count=None):
    """
    Create dtype_register dictionary. Equates to a row in the metadata file.

    """
    return {
        "DTYPE_ID": dtype_id,
        "DTYPE_NAME": dtype_name,
        "NETWORK_ID": network_id,
        "DTYPE_DESC": dtype_desc,
        "UNITS": units,
        "MEAN_MIN": value_min,
        "MEAN_PERCENTILE_20": percentile_20,
        "MEAN_PERCENTILE_40": percentile_40,
        "MEAN_PERCENTILE_60": percentile_60,
        "MEAN_PERCENTILE_80": percentile_80,
        "MEAN_MAX": value_max,
        "MEAN_MEAN": value_mean,
        "MEAN_COUNT": value_count,
    }


def make_avail_dict(site_id=None, network_id=None, dtype_id=None,
                    start_date=None, end_date=None, value_count=None,
                    value_mean=None):
    """
    Create data_availability dictionary. Equates to a row in the metadata file.

    """
    return {
        "SITE_ID": site_id,
        "NETWORK_ID": network_id,
        "DTYPE_ID": dtype_id,
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SITE_VALUE_COUNT": value_count,
        "SITE_VALUE_MEAN": value_mean,
    }


def _add_dtype_value_stats(dtype_dict, dtype_values):
    dtype_values = np.array(dtype_values)
    dtype_dict["MEAN_MIN"] = dtype_values.min()
    dtype_dict["MEAN_PERCENTILE_20"] = np.percentile(dtype_values, 20)
    dtype_dict["MEAN_PERCENTILE_40"] = np.percentile(dtype_values, 40)
    dtype_dict["MEAN_PERCENTILE_60"] = np.percentile(dtype_values, 60)
    dtype_dict["MEAN_PERCENTILE_80"] = np.percentile(dtype_values, 80)
    dtype_dict["MEAN_MAX"] = dtype_values.max()
    dtype_dict["MEAN_MEAN"] = round(dtype_values.mean(), 2)
    dtype_dict["MEAN_COUNT"] = len(dtype_values)

    return dtype_dict


def _add_dtype_stats(avail_rows, dtype_dicts):
    """
    Gather all the site means for each data type and caclulate stats.

    """
    dtype_values_dict = {}
    for avail_dict in avail_rows:
        if avail_dict["SITE_VALUE_MEAN"] is None:
            continue

        dtype_id = avail_dict["DTYPE_ID"]
        if dtype_id not in dtype_values_dict:
            dtype_values_dict[dtype_id] = []
        dtype_values_dict[dtype_id].append(avail_dict["SITE_VALUE_MEAN"])

    for dtype_id, site_means in dtype_values_dict.items():
        dtype_dicts[dtype_id] = _add_dtype_value_stats(dtype_dicts[dtype_id],
                                                       site_means)

    return dtype_dicts


def _str_to_date(date_string, date_format, default_now=False):
    if default_now is True and date_string is None:
        dt = datetime.now()
    elif date_string is not None:
        dt = datetime.strptime(date_string, date_format)
    else:
        dt = date_string

    return dt


# *** EA water quality ********************************************************
"""
Data for nitrate and phosphate EA water quality (EA_WQ) samples from API.

"""
EA_WQ_API_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
EA_WQ_DTYE_IDS = [
    "0117",
    "0180"
]

EA_WQ_MATERIALS = [
    "%s/def/sampled-material-types/2AZZ" % paths.EA_WQ_ID_URL, # RIVER / RUNNING SURFACE WATER
    "%s/def/sampled-material-types/2GZZ" % paths.EA_WQ_ID_URL, # POND / LAKE / RESERVOIR WATER
    "%s/def/sampled-material-types/2HZZ" % paths.EA_WQ_ID_URL, # ESTUARINE WATER
]


def create_EA_WQ_metadata(limit_calls=None):
    """
    Create metatdata on the sites, data types and data availability for EA
    water quality (EA_WQ).
    We just look at data type info for nitrate and phosphate.
    Data is collected from the API and saved to CSV.

    """
    print("EA_WQ metadata")
    # Site and data metadata --------------------------------------------------
    # Set up coordinate transformation
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    limit = 500
    offset = 0

    dtype_rows = {}
    sites_rows = {}
    avail_rows = {}
    measure_values = {}

    finished = False
    calls = 0
    while finished is False:
        # Query string
        materials = "&sampledMaterialType=".join(EA_WQ_MATERIALS)
        det_ids = "&determinand=".join(EA_WQ_DTYE_IDS)
        query = "_limit=%s&_offset=%s&determinand=%s&sampledMaterialType=%s" \
                % (limit, offset, det_ids, materials)

        # Full URL
        url = "%s/data/measurement?%s" % (paths.EA_WQ_API_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Measurement call %s to %s" % (offset, offset + limit))
            calls += 1
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites.
            if len(data["items"]) < limit:
                finished = True

            if limit_calls is not None and limit_calls == calls:
                finished = True

            for measure_info in data["items"]:
                # Site data
                site_info = measure_info["sample"]["samplingPoint"]
                site_id = site_info["notation"]
                if site_id not in sites_rows:
                    lat, long = transformer.transform(site_info["easting"],
                                                      site_info["northing"])
                    site_dict = make_site_dict(site_id=site_id,
                                               site_name=site_info["label"],
                                               network_id=config.EA_WQ_ID,
                                               lat=lat,
                                               long=long)
                    sites_rows[site_id] = site_dict

                # Data type data
                dtype_info = measure_info["determinand"]
                dtype_id = dtype_info["notation"]
                if dtype_id not in dtype_rows:
                    dtype_dict = make_dtype_dict(
                        dtype_id=dtype_id,
                        dtype_name=dtype_info["label"],
                        network_id=config.EA_WQ_ID,
                        dtype_desc=dtype_info["definition"],
                        units=dtype_info["unit"]["label"])
                    dtype_rows[dtype_id] = dtype_dict

                # Availability stats
                sample_date = datetime.strptime(
                    measure_info["sample"]["sampleDateTime"],
                    EA_WQ_API_DATE_FORMAT)

                # Set up dictionaries for measurements and availability
                if site_id not in measure_values:
                    measure_values[site_id] = {}
                    avail_rows[site_id] = {}
                if dtype_id not in measure_values[site_id]:
                    measure_values[site_id][dtype_id] = []
                    avail_rows[site_id][dtype_id] = None

                # Collect the actual data
                measure_values[site_id][dtype_id].append(
                    measure_info["result"])

                if avail_rows[site_id][dtype_id] is None:
                    avail_dict = make_avail_dict(site_id=site_id,
                                                 network_id=config.EA_WQ_ID,
                                                 dtype_id=dtype_id,
                                                 start_date=sample_date,
                                                 end_date=sample_date)
                    avail_rows[site_id][dtype_id] = avail_dict

                else:
                    if sample_date < avail_rows[site_id][dtype_id]["START_DATE"]:
                        avail_rows[site_id][dtype_id]["START_DATE"] = sample_date
                    elif sample_date > avail_rows[site_id][dtype_id]["END_DATE"]:
                        avail_rows[site_id][dtype_id]["END_DATE"] = sample_date

        offset += limit

    # Go through measurements and work out stats
    for site_id, site_dict in measure_values.items():
        for dtype_id, measurements in site_dict.items():
            site_mean = round(np.array(measurements).mean(), 2)
            site_count = len(measurements)
            avail_rows[site_id][dtype_id]["SITE_VALUE_MEAN"] = site_mean
            avail_rows[site_id][dtype_id]["SITE_VALUE_COUNT"] = site_count

    # Flatten availability dictionaries into list
    avail_rows_flat = []
    for site_dict in avail_rows.values():
        for dtype_dict in site_dict.values():
            avail_rows_flat.append(dtype_dict)

    dtype_rows = _add_dtype_stats(avail_rows_flat, dtype_rows)

    # Append to exisiting CSV
    pd.DataFrame(sites_rows.values()).to_csv(
        paths.SITE_REGISTER_FPATH.format(NETWORK=config.EA_WQ_ID),
        na_rep=None,
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(avail_rows_flat).to_csv(
        paths.DATA_AVAILABILITY_FPATH.format(NETWORK=config.EA_WQ_ID),
         na_rep=None,
         index=False,
         date_format=config.DATE_FORMAT)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_WQ_ID),
        index=False,
        date_format=config.DATE_FORMAT)




# *** EA BioSys ***************************************************************
"""
Site and availability data for EA BioSys datasets (EA_INV, EA_MACP, EA_DIAT)
fetched from CSV.

"""
EA_INV_DTYPE_DICT = {
    "BMWP_N_TAXA": {
        "name": "Number of taxa contributing to the BMWP index",
        "desc": None,
        "unit": "count",
    },
    "BMWP_TOTAL": {
        "name": "BMWP index total score",
        "desc": None,
        "unit": "total score",
    },
    "BMWP_ASPT": {
        "name": "BMWP index Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
    "CCI_N_TAXA": {
        "name": "Number of taxa contributing to the CCI index",
        "desc": None,
        "unit": "count",
    },
    "CCI_CS_TOTAL": {
        "name": "CCI index total score",
        "desc": None,
        "unit": "total score",
    },
    "CCI_ASPT": {
        "name": "CCI index Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
    "CSmax_CoS": {
        "name": "Conservation score of the rarest taxon in sample",
        "desc": "Part of the CCI index calculations - it displays the "
                "conservation score of the rarest taxon in the sample.",
        "unit": "conservation score",
    },
    "BMWP_CoS": {
        "name": "Conservation Score derived from the BMWP score",
        "desc": "Part of the CCI index calculations - it displays the "
                "Conservation Score derived from the BMWP score.",
        "unit": "conservation score",
    },
    "CCI_CoS": {
        "name": "Highest conservation score",
        "desc": "This displays the highest Conservation Score (from either "
                "the rarest taxon or the BMWP range) and is used in the final "
                "CCI calculation.",
        "unit": "conservation score",
    },
    "CCI": {
        "name": "Community Conservation Index",
        "desc": "The final Community Conservation Index.",
        "unit": "score",
    },
    "DEHLI_N_TAXA": {
        "name": "Number of taxa contributing to the DEHLI index",
        "desc": None,
        "unit": "count",
    },
    "DIS_TOTAL": {
        "name": "DEHLI index total score",
        "desc": None,
        "unit": "total score",
    },
    "DEHLI": {
        "name": "Final DEHLI index",
        "desc": None,
        "unit": "score",
    },
    "EPSI_ML_S_GRP": {
        "name": "Number of Mixed-level E-PSI sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_ML_ALL_GRP": {
        "name": "Number of Mixed-level E-PSI sensitive and insensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_MIXED_LEVEL_SCORE": {
        "name": "Mixed-level E-PSI index",
        "desc": None,
        "unit": "score",
    },
    "EPSI_S_GRP": {
        "name": "Number of family-level PSI sensitive/fairly sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_ALL_GRP": {
        "name": "Number of family-level PSI scoring taxa",
        "desc": None,
        "unit": "count",
    },
    "EPSI_FAMILY_SCORE": {
        "name": "Family-level PSI index",
        "desc": None,
        "unit": "score",
    },
    "LIFE_N_TAXA": {
        "name": "Number of taxa contributing to Family LIFE index",
        "desc": None,
        "unit": "count",
    },
    "LIFE_SCORES_TOTAL": {
        "name": "Family LIFE index total score",
        "desc": None,
        "unit": "total score",
    },
    "LIFE_FAMILY_INDEX": {
        "name": "Family LIFE index",
        "desc": None,
        "unit": "score",
    },
    "LIFE_SPECIES_N_TAXA": {
        "name": "Number of taxa contributing to the Species LIFE index",
        "desc": None,
        "unit": "count",
    },
    "LIFE_SPECIES_SCORES_TOTAL": {
        "name": "Species LIFE index total score",
        "desc": None,
        "unit": "total score",
    },
    "LIFE_SPECIES_INDEX": {
        "name": "Species LIFE index",
        "desc": None,
        "unit": "score",
    },
    "PSI_ML_AB": {
        "name": "Number of mixed-level PSI sensitive/fairly sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_ML_ABCD": {
        "name": "Number of mixed-level PSI scoring taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_MIXED_LEVEL_SCORE": {
        "name": "Mixed-level PSI index",
        "desc": None,
        "unit": "score",
    },
    "PSI_AB": {
        "name": "Number of family-level PSI sensitive/fairly sensitive taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_ABCD": {
        "name": "Number of family-level PSI scoring taxa",
        "desc": None,
        "unit": "count",
    },
    "PSI_FAMILY_SCORE": {
        "name": "Family-level PSI index",
        "desc": None,
        "unit": "score",
    },
    "WHPT_N_TAXA": {
        "name": "Number of taxa contributing to the WHPT index",
        "desc": None,
        "unit": "count",
    },
    "WHPT_TOTAL": {
        "name": "WHPT index total score",
        "desc": None,
        "unit": "total score",
    },
    "WHPT_ASPT": {
        "name": "WHPT index Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
    "WHPT_NW_N_TAXA": {
        "name": "Number of taxa contributing to the non-abundance weighted "
                "WHPT index",
        "desc": None,
        "unit": "count",
    },
    "WHPT_NW_TOTAL": {
        "name": "Non-abundance weighted WHPT index total score",
        "desc": "Note: Do not confuse this with the more commonly used WHPT "
                "index which includes abundance",
        "unit": "total score",
    },
    "WHPT_NW_ASPT": {
        "name": "Non-abundance weighted WHPT Average Score Per Taxon",
        "desc": None,
        "unit": "average score per taxon",
    },
}

EA_MACP_DTYPE_DICT = {
    "RMHI": {
        "name": "River Macrophyte Hydraulic index",
        "desc": None,
        "unit": "score",
    },
    "RMNI": {
        "name": "River Macrophyte Nutrient Index",
        "desc": None,
        "unit": "score",
    },
    "RN_A_TAXA": {
        "name": "Number of aquatic taxa contributing to the RMNI index",
        "desc": None,
        "unit": "count",
    },
    "N_RFG": {
        "name": "Number of river macrophyte functional groups",
        "desc": None,
        "unit": "count",
    },
    "RFA_PC": {
        "name": "Percentage cover of Filamentous algae",
        "desc": None,
        "unit": "%",
    },
}

EA_DIAT_DTYPE_DICT = {
    "TOTAL_UNITS_FOUND_LM": {
        "name": "Total valves in analysis using light microscopy",
        "desc": "The total number of valves in the analysis counted using light microscopy",
        "unit": "count",
    },
    "TDI4": {
        "name": "Trophic Diatom Index 4",
        "desc": "This is only calculated for samples analysed before "
                "07/03/2017 using a light microscope.",
        "unit": "score",
    },
    "TOTAL_TDI4_CELLS": {
        "name": "Total number of TDI4 diatom scoring taxa valves in sample",
        "desc": None,
        "unit": "count",
    },
    "TDI4_PERCENT_PLANKTONIC": {
        "name": "Percent of planktonic valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI4_PERCENT_MOTILE": {
        "name": "Percent of motile valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI4_PERCENT_PTV": {
        "name": "Percent of organic/pollution tolerant valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI4_PERCENT_SALINITY": {
        "name": "Percent of saline tolerant valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5LM": {
        "name": "Trophic Diatom Index 5",
        "desc": "This is only calculated for samples analysed under a light "
                "microscope.",
        "unit": "score",
    },
    "TOTAL_TDI5LM_CELLS": {
        "name": "Total number of TDI5 diatom scoring taxa valves in sample",
        "desc": None,
        "unit": "count",
    },
    "TDI5_LM_PERCENT_PLANKTONIC": {
        "name": "Percent of planktonic valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5_LM_PERCENT_MOTILE": {
        "name": "Percent of motile valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5_LM_PERCENT_PTV": {
        "name": "Percent of organic/pollution tolerant valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5_LM_PERCENT_SALINITY": {
        "name": "Percent of saline tolerant valves in sample",
        "desc": None,
        "unit": "%",
    },
    "TOTAL_NGS_SEQUENCE_READS": {
        "name": "Total number of sequence reads per taxon in sample",
        "desc": None,
        "unit": "count",
    },
    "TOTAL_TDI5_NGS_SEQUENCE_READS": {
        "name": "Total number of sequence reads per TDI5-Scoring taxa in sample",
        "desc": None,
        "unit": "count",
    },
    "TDI5_NGS_PERCENT_PLANKTONIC": {
        "name": "Percent of planktonic valves in the NGS sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5_NGS_PERCENT_MOTILE": {
        "name": "Percent of motile valves in the NGS sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5_NGS_PERCENT_PTV": {
        "name": "Percent of organic/pollution tolerant valves in the NGS sample",
        "desc": None,
        "unit": "%",
    },
    "TDI5_NGS_PERCENT_SALINITY": {
        "name": "Percent of saline tolerant valves in the NGS sample",
        "desc": None,
        "unit": "%",
    },
}


def _create_EA_BIO_metadata(bio_id, metric_fpath, site_fpath, dtype_dicts):
    """
    Fetch site, data availability and data type info for BioSys data
    and save to file.

    """
    # Create data types data
    dtype_rows = {}
    dtype_ids = []
    for dtype_id, dtype_info in dtype_dicts.items():
        dtype_ids.append(dtype_id)
        dtype_dict = make_dtype_dict(dtype_id=dtype_id,
                                     dtype_name=dtype_info["name"],
                                     network_id=bio_id,
                                     dtype_desc=dtype_info["desc"],
                                     units=dtype_info["unit"])
        dtype_rows[dtype_id] = dtype_dict

    data_usecols = ["SITE_ID", "SAMPLE_DATE"] + dtype_ids
    sites_usecols = ["SITE_ID", "WATER_BODY", "FULL_EASTING", "FULL_NORTHING"]

    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    sites_rows = []
    avail_rows = []

    data = pd.read_csv(metric_fpath, usecols=data_usecols)
    sites = pd.read_csv(site_fpath, usecols=sites_usecols)

    for site_id in data["SITE_ID"].unique():
        site_data = data.loc[data["SITE_ID"] == site_id]
        site_info = sites.loc[sites["SITE_ID"] == site_id].iloc[0]

        lat, long = transformer.transform(site_info["FULL_EASTING"],
                                          site_info["FULL_NORTHING"])

        # Create site data
        site_dict = make_site_dict(
            site_id=site_id,
            site_name=site_info["WATER_BODY"],
            network_id=bio_id,
            lat=lat,
            long=long
        )

        sites_rows.append(site_dict)

        # Sort dtypes and data availaibility
        for dtype_id in dtype_ids:
            site_dtype = site_data.loc[site_data[dtype_id].notnull(),
                                       ["SAMPLE_DATE", dtype_id]]

            if len(site_dtype) == 0:
                continue

            dtype_mean = round(site_dtype[dtype_id].mean(), 2)

            avail_dict = make_avail_dict(
                site_id=site_id,
                network_id=bio_id,
                dtype_id=dtype_id,
                start_date=site_dtype["SAMPLE_DATE"].min(),
                end_date=site_dtype["SAMPLE_DATE"].max(),
                value_count=len(site_dtype),
                value_mean=dtype_mean
            )
            avail_rows.append(avail_dict)

    dtype_rows = _add_dtype_stats(avail_rows, dtype_rows)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=bio_id),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(sites_rows).to_csv(
        paths.SITE_REGISTER_FPATH.format(NETWORK=bio_id),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(avail_rows).to_csv(
        paths.DATA_AVAILABILITY_FPATH.format(NETWORK=bio_id),
        index=False,
        date_format=config.DATE_FORMAT)


def create_all_EA_BIO_metadata():
    print("EA_INV metadata")
    _create_EA_BIO_metadata(config.EA_INV_ID, paths.EA_INV_METRICS_RAW_FILE,
                            paths.EA_INV_SITE_RAW_FILE, EA_INV_DTYPE_DICT)

    print("EA_MACP metadata")
    _create_EA_BIO_metadata(config.EA_MACP_ID, paths.EA_MACP_METRICS_RAW_FILE,
                            paths.EA_MACP_SITE_RAW_FILE, EA_MACP_DTYPE_DICT)

    print("EA_DIAT metadata")
    _create_EA_BIO_metadata(config.EA_DIAT_ID, paths.EA_DIAT_METRICS_RAW_FILE,
                            paths.EA_DIAT_SITE_RAW_FILE, EA_DIAT_DTYPE_DICT)



# *** EA Fish *****************************************************************
"""
Site and availability data for EA Fish count (EA_FISH) fetched from CSV.

"""
EA_FISH_DTYPE_DICT = {
    "FISH_COUNT": {
        "name": "Total fish count",
        "desc": None,
        "unit": "count",
    }
}


def create_EA_FISH_metadata():
    """
    Fetch site, data availability and data type info for BioSys data
    and save to file.

    """
    print("EA_FISH metadata")
    # Create data types data
    dtype_rows = {}
    dtype_ids = []
    for dtype_id, dtype_info in EA_FISH_DTYPE_DICT.items():
        dtype_ids.append(dtype_id)
        dtype_dict = make_dtype_dict(dtype_id=dtype_id,
                                     dtype_name=dtype_info["name"],
                                     network_id=config.EA_FISH_ID,
                                     dtype_desc=dtype_info["desc"],
                                     units=dtype_info["unit"])
        dtype_rows[dtype_id] = dtype_dict

    usecols = ["SITE_ID", "SITE_NAME", "SURVEY_ID", "EVENT_DATE",
               "SURVEY_RANKED_EASTING", "SURVEY_RANKED_NORTHING", "ALL_RUNS"]

    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    sites_rows = []
    avail_rows = []

    data = pd.read_csv(paths.EA_FISH_RAW_FILE, usecols=usecols)

    for site_id in data["SITE_ID"].unique():
        site_data = data.loc[data["SITE_ID"] == site_id]
        site_data["EVENT_DATE"] = pd.to_datetime(site_data["EVENT_DATE"])

        lat, long = transformer.transform(
            site_data["SURVEY_RANKED_EASTING"].values[0],
            site_data["SURVEY_RANKED_NORTHING"].values[0])

        # Create site data
        site_dict = make_site_dict(
            site_id=site_id,
            site_name=site_data["SITE_NAME"].values[0],
            network_id=config.EA_FISH_ID,
            lat=lat,
            long=long
        )

        sites_rows.append(site_dict)

        site_values = []
        # Sort data availaibility (only hanle counts)
        for survey_id in site_data["SURVEY_ID"].unique():
            survey_data = site_data.loc[site_data["SURVEY_ID"] == survey_id]
            count_total = survey_data["ALL_RUNS"].sum()
            site_values.append(count_total)

        avail_dict = make_avail_dict(
            site_id=site_id,
            network_id=config.EA_FISH_ID,
            dtype_id=dtype_id,
            start_date=site_data["EVENT_DATE"].min(),
            end_date=site_data["EVENT_DATE"].max(),
            value_count=len(site_values),
            value_mean=round(np.array(site_values).mean(), 2)
        )
        avail_rows.append(avail_dict)

    dtype_rows = _add_dtype_stats(avail_rows, dtype_rows)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_FISH_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(sites_rows).to_csv(
        paths.SITE_REGISTER_FPATH.format(NETWORK=config.EA_FISH_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(avail_rows).to_csv(
        paths.DATA_AVAILABILITY_FPATH.format(NETWORK=config.EA_FISH_ID),
        index=False,
        date_format=config.DATE_FORMAT)


# *** Riverflies **************************************************************
"""
Site and availability data for Riverflies (RF) fetched from CSV.

"""
RF_DTYPE_DICT = {
    "Cased caddisfly score": {
        "name": "Cased caddisfly score",
        "desc": "",
        "unit": "score",
    },
    "Cased caddisfly Num": {
        "name": "Cased caddisfly Num",
        "desc": "",
        "unit": "count",
    },
    "Caseless caddisfly score": {
        "name": "Caseless caddisfly score",
        "desc": "",
        "unit": "score",
    },
    "Caseless caddisfly Num": {
        "name": "Caseless caddisfly Num",
        "desc": "",
        "unit": "count",
    },
    "Ephemeridae score": {
        "name": "Ephemeridae score",
        "desc": "",
        "unit": "score",
    },
    "Ephemeridae Num": {
        "name": "Ephemeridae Num",
        "desc": "",
        "unit": "count",
    },
    "Ephemerellidae score": {
        "name": "Ephemerellidae score",
        "desc": "",
        "unit": "score",
    },
    "Ephemerellidae Num": {
        "name": "Ephemerellidae Num",
        "desc": "",
        "unit": "count",
    },
    "Heptageniidae score": {
        "name": "Heptageniidae score",
        "desc": "",
        "unit": "score",
    },
    "Heptageniidae Num": {
        "name": "Heptageniidae Num",
        "desc": "",
        "unit": "count",
    },
    "Baetidae score": {
        "name": "Baetidae score",
        "desc": "",
        "unit": "score",
    },
    "Baetidae Num": {
        "name": "Baetidae Num",
        "desc": "",
        "unit": "count",
    },
    "Stoneflies score": {
        "name": "Stoneflies score",
        "desc": "",
        "unit": "score",
    },
    "Stoneflies Num": {
        "name": "Stoneflies Num",
        "desc": "",
        "unit": "count",
    },
    "Gammarus score": {
        "name": "Gammarus score",
        "desc": "",
        "unit": "score",
    },
    "Gammarus Num": {
        "name": "Gammarus Num",
        "desc": "",
        "unit": "count",
    },
}


def create_RF_metadata():
    """
    Fetch site, data availability and data type info for Riverflies (RF) data
    and save to file.

    """
    print("RF metadata")
    # Create data types data
    dtype_rows = {}
    dtype_ids = []
    for dtype_id, dtype_info in RF_DTYPE_DICT.items():
        dtype_ids.append(dtype_id)
        dtype_dict = make_dtype_dict(dtype_id=dtype_id,
                                     dtype_name=dtype_info["name"],
                                     network_id=config.RF_ID,
                                     dtype_desc=dtype_info["desc"],
                                     units=dtype_info["unit"])
        dtype_rows[dtype_id] = dtype_dict

    usecols = ["Site", "River", "Date", "Time", "Lat", "Long"] + dtype_ids

    sites_rows = []
    avail_rows = []

    for filename in os.listdir(paths.RF_RAW_DIR):
        file_path = os.path.join(paths.RF_RAW_DIR, filename)

        f_ext = filename.split(".")[-1]
        if f_ext == "csv":
            rf_data = pd.read_csv(file_path, usecols=usecols)
        elif f_ext == "xlsx":
            rf_data = pd.read_excel(file_path, header=1, usecols=usecols)
        else:
            continue

        print(filename)
        rf_data["Site_id"] = rf_data["Site"] + " on " + rf_data["River"]

        if rf_data["Date"].dtypes == "datetime64[ns]":
            rf_data["Date"] = rf_data["Date"].dt.strftime('%Y-%m-%d')

        rf_data["DateTime"] = pd.to_datetime(
            rf_data["Date"] + " " + rf_data["Time"])

        for site_id in rf_data["Site_id"].unique():

            rf_site = rf_data.loc[rf_data["Site_id"] == site_id]
            if len(rf_site) == 0:
                continue

            # Create site data
            site_dict = make_site_dict(
                site_id=site_id,
                site_name=rf_site.iloc[0]["Site"],
                network_id=config.RF_ID,
                lat=rf_site.iloc[0]["Lat"],
                long=rf_site.iloc[0]["Long"]
            )

            sites_rows.append(site_dict)

            # Sort dtype and data availaibility
            for dtype_id in dtype_ids:
                rf_site_dtype = rf_site.loc[rf_site[dtype_id].notnull(),
                                            ["DateTime", dtype_id]]

                if len(rf_site_dtype) == 0:
                    continue

                dtype_mean = round(rf_site_dtype[dtype_id].mean(), 2)

                avail_dict = make_avail_dict(
                    site_id=site_id,
                    network_id=config.RF_ID,
                    dtype_id=dtype_id,
                    start_date=rf_site_dtype["DateTime"].min(),
                    end_date=rf_site_dtype["DateTime"].max(),
                    value_count=len(rf_site_dtype),
                    value_mean=dtype_mean
                )
                avail_rows.append(avail_dict)

    dtype_rows = _add_dtype_stats(avail_rows, dtype_rows)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.RF_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(sites_rows).to_csv(
        paths.SITE_REGISTER_FPATH.format(NETWORK=config.RF_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(avail_rows).to_csv(
        paths.DATA_AVAILABILITY_FPATH.format(NETWORK=config.RF_ID),
        index=False,
        date_format=config.DATE_FORMAT)



# *** Fresh water watch *******************************************************
"""
Site and availability data for fresh water watch (FWW) fetched from CSV.

"""
FWW_DTYPE_DICT = {
    "Nitrate": {
        "name": "Nitrate",
        "desc": None,
        "unit": "mg/l",
    },
    "Phosphate": {
        "name": "Phosphate",
        "desc": None,
        "unit": "mg/l",
    },
}

def create_FWW_metadata():
    """
    Fetch site, data availability and data type info for fresh water watch
    (FWW) data and save to file.

    """
    print("FWW metadata")
    # Create data types data
    dtype_rows = {}
    dtype_ids = []
    for dtype_id, dtype_info in FWW_DTYPE_DICT.items():
        dtype_ids.append(dtype_id)
        dtype_dict = make_dtype_dict(dtype_id=dtype_id,
                                     dtype_name=dtype_info["name"],
                                     network_id=config.FWW_ID,
                                     dtype_desc=dtype_info["desc"],
                                     units=dtype_info["unit"])
        dtype_rows[dtype_id] = dtype_dict

    usecols = ["site_name", "sample_date", "lat", "lng"] + dtype_ids

    sites_rows = []
    avail_rows = []

    fww_data = pd.read_csv(paths.FWW_RAW_FILE, usecols=usecols)

    for site_id in fww_data["site_name"].unique():

        fww_site = fww_data.loc[fww_data["site_name"] == site_id]
        if len(fww_site) == 0:
            continue

        if pd.isnull(fww_site.iloc[0]["lat"]) or \
                pd.isnull(fww_site.iloc[0]["lng"]):
            # Connaot continue without coordinates
            continue

        # Create site data
        site_dict = make_site_dict(
            site_id=site_id,
            site_name=site_id,
            network_id=config.FWW_ID,
            lat=fww_site.iloc[0]["lat"],
            long=fww_site.iloc[0]["lng"]
        )

        sites_rows.append(site_dict)

        # Sort dtypes and data availaibility
        for dtype_id in dtype_ids:
            fww_site_dtype = fww_site.loc[fww_site[dtype_id].notnull(),
                                          ["sample_date", dtype_id]]

            if len(fww_site_dtype) == 0:
                continue

            dtype_mean = round(fww_site_dtype[dtype_id].mean(), 2)

            avail_dict = make_avail_dict(
                site_id=site_id,
                network_id=config.FWW_ID,
                dtype_id=dtype_id,
                start_date=fww_site_dtype["sample_date"].min(),
                end_date=fww_site_dtype["sample_date"].max(),
                value_count=len(fww_site_dtype),
                value_mean=dtype_mean
            )
            avail_rows.append(avail_dict)

    dtype_rows = _add_dtype_stats(avail_rows, dtype_rows)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.FWW_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(sites_rows).to_csv(
        paths.SITE_REGISTER_FPATH.format(NETWORK=config.FWW_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(avail_rows).to_csv(
        paths.DATA_AVAILABILITY_FPATH.format(NETWORK=config.FWW_ID),
        index=False,
        date_format=config.DATE_FORMAT)


# *** National river flow archive *********************************************
"""
Site and availability data for National river flow archive (NRFA) fetched from
API.

"""
NRFA_DATE_FORMAT = "%Y-%m-%d"
NRFA_DTYPE_DICT = {
    "gdf": {
        "name": "Gauged daily flows",
        "desc": None,
        "unit": "m3/s",
    },
    "cdr": {
        "name": "Catchment daily rainfall",
        "desc": None,
        "unit": "mm",
    },
    "pot-flow": {
        "name": "Peaks over threshold flow",
        "desc": None,
        "unit": "m3/s",
    },
    "amax-stage": {
        "name": "Annual maxima stage",
        "desc": None,
        "unit": "m",
    },
    "amax-flow": {
        "name": "Annual maxima flow",
        "desc": None,
        "unit": "m3/s",
    },
}

def create_NRFA_metadata():
    """
    Fetch site, data availability and data type info for fresh water watch
    (FWW) data and save to file.

    """
    print("NRFA metadata")
    # Create data types data
    dtype_rows = {}
    for dtype_id, dtype_info in NRFA_DTYPE_DICT.items():
        dtype_dict = make_dtype_dict(dtype_id=dtype_id,
                                     dtype_name=dtype_info["name"],
                                     network_id=config.NRFA_ID,
                                     dtype_desc=dtype_info["desc"],
                                     units=dtype_info["unit"])
        dtype_rows[dtype_id] = dtype_dict

    # Site and data metadata --------------------------------------------------
    sites_rows = []
    site_ids = []
    avail_rows = []

    # Query string
    query = "format=json-object&station=*&fields=id,name,lat-long," \
            "gdf-statistics,peak-flow-statistics"
    # Full URL
    url = "%s/station-info?%s" % (paths.NRFA_API_URL, query)

    with urllib.request.urlopen(url) as response:
        data = json.load(response)
        for site_info in data["data"]:
            site = make_site_dict(site_id=site_info["id"],
                                  site_name=site_info["name"],
                                  network_id=config.NRFA_ID,
                                  lat=site_info["lat-long"]["latitude"],
                                  long=site_info["lat-long"]["longitude"])

            sites_rows.append(site)
            site_ids.append(site_info["id"])

            # Sort dates
            gdf_start_date = _str_to_date(site_info["gdf-start-date"],
                                          NRFA_DATE_FORMAT)
            gdf_end_date = _str_to_date(site_info["gdf-end-date"],
                                        NRFA_DATE_FORMAT, default_now=True)
            pot_start_date = _str_to_date(site_info["peak-flow-start-date"],
                                          NRFA_DATE_FORMAT)
            pot_end_date = _str_to_date(site_info["peak-flow-end-date"],
                                        NRFA_DATE_FORMAT, default_now=True)

            if gdf_start_date is not None:
                # GDF availability
                gdf_avail_dict = make_avail_dict(
                    site_id=site_info["id"],
                    network_id=config.NRFA_ID,
                    dtype_id="gdf",
                    start_date=gdf_start_date,
                    end_date=gdf_end_date,
                    value_count=site_info["gdf-flow-count"],
                    value_mean=site_info["gdf-mean-flow"])
                avail_rows.append(gdf_avail_dict)

                # CDF availability (mean and count not available, will add later
                # from file)
                cdr_avail_dict = make_avail_dict(
                    site_id=site_info["id"],
                    network_id=config.NRFA_ID,
                    dtype_id="cdr",
                    start_date=gdf_start_date,
                    end_date=gdf_end_date)
                avail_rows.append(cdr_avail_dict)

            if pot_start_date is not None:
                # Peak flow availability
                pot_avail_dict = make_avail_dict(
                    site_id=site_info["id"],
                    network_id=config.NRFA_ID,
                    dtype_id="pot-flow",
                    start_date=pot_start_date,
                    end_date=pot_end_date)
                avail_rows.append(pot_avail_dict)

    # Add additional data from file
    cdr_mean_data = pd.read_csv(paths.NRFA_CDR_DATA_FILE)
    checked_avail_rows = []
    for avail_dict in avail_rows:
        if avail_dict["DTYPE_ID"] == "cdr":
            site_id = avail_dict["SITE_ID"]
            mean_cdr = cdr_mean_data.loc[cdr_mean_data["STATION"] == site_id,
                                         "mean_rainfall"]
            if len(mean_cdr) > 0:
                avail_dict["SITE_VALUE_MEAN"] = mean_cdr.values[0]
                checked_avail_rows.append(avail_dict)
        else:
            checked_avail_rows.append(avail_dict)

    amax_mean_data = pd.read_csv(paths.NRFA_AMAX_DATA_FILE)
    amax_mean_data["start"] = pd.to_datetime(amax_mean_data["start"])
    amax_mean_data["end"] = pd.to_datetime(amax_mean_data["end"])

    for i, row in amax_mean_data.iterrows():
        if row["STATION"] not in site_ids:
            continue

        # AMAX stage
        if pd.notnull(row["mean_amax_stage"]):
            amst_avail_dict = make_avail_dict(
                site_id=row["STATION"],
                network_id=config.NRFA_ID,
                dtype_id="amax-stage",
                start_date=row["start"],
                end_date=row["end"],
                value_mean=row["mean_amax_stage"])
            checked_avail_rows.append(amst_avail_dict)

        # AMAX flow
        if pd.notnull(row["mean_amax_flow"]):
            amfl_avail_dict = make_avail_dict(
                site_id=row["STATION"],
                network_id=config.NRFA_ID,
                dtype_id="amax-flow",
                start_date=row["start"],
                end_date=row["end"],
                value_mean=row["mean_amax_flow"])
            checked_avail_rows.append(amfl_avail_dict)

    dtype_rows = _add_dtype_stats(checked_avail_rows, dtype_rows)

    pd.DataFrame(dtype_rows.values()).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.NRFA_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(sites_rows).to_csv(
        paths.SITE_REGISTER_FPATH.format(NETWORK=config.NRFA_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    pd.DataFrame(checked_avail_rows).to_csv(
        paths.DATA_AVAILABILITY_FPATH.format(NETWORK=config.NRFA_ID),
        index=False,
        date_format=config.DATE_FORMAT)


def create_all_metadata_csv():
    """
    Run all the metadata extracter functions.
    This will take a while.

    """
    create_EA_WQ_metadata()
    create_all_EA_BIO_metadata()
    create_EA_FISH_metadata()
    create_RF_metadata()
    create_FWW_metadata()
    create_NRFA_metadata()


# *** General network info ****************************************************
def _try_round(value, dcp):
    if value is None:
        return value
    else:
        return round(value, dcp)


def _get_dtype_dicts(network_id):
    dtypes_info = pd.read_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=network_id),
        dtype={"DTYPE_ID": str})
    dtypes_info = dtypes_info.replace({np.nan: None})

    dtype_dicts = []
    for i, dtype in dtypes_info.iterrows():
        dtype_dict = {
            "dtype_id": dtype["DTYPE_ID"],
            "dtype_name": dtype["DTYPE_NAME"],
            "dtype_desc": dtype["DTYPE_DESC"],
            "network_id": dtype["NETWORK_ID"],
            "mean_min": _try_round(dtype["MEAN_MIN"], 2),
            "percentile_20": _try_round(dtype["MEAN_PERCENTILE_20"], 2),
            "percentile_40": _try_round(dtype["MEAN_PERCENTILE_40"], 2),
            "percentile_60": _try_round(dtype["MEAN_PERCENTILE_60"], 2),
            "percentile_80": _try_round(dtype["MEAN_PERCENTILE_80"], 2),
            "mean_max": _try_round(dtype["MEAN_MAX"], 2),
            "mean_mean": _try_round(dtype["MEAN_MEAN"], 2),
            "mean_count": dtype["MEAN_COUNT"],
        }
        dtype_dicts.append(dtype_dict)

    return dtype_dicts


def _to_json_format(dict):
    json_dict = {}
    for key, value in dict.items():
        json_dict[key.lower()] = value

    return json_dict


def create_network_json():
    """
    Save general metadata for all networks.

    """
    network_json = []

    # EA WQ
    EA_WQ_dict = make_network_dict(
        network_id=config.EA_WQ_ID,
        network_name="EA Water Quality",
        network_desc="EA Water quality monitoring network",
        folder="wq",
        shape="square",
        access="geojson",
        updates="Realtime data available",
        website=paths.EA_WQ_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.EA_WQ_ID),
    )
    network_json.append(_to_json_format(EA_WQ_dict))

    # EA INV
    EA_INV_dict = make_network_dict(
        network_id=config.EA_INV_ID,
        network_name="EA Macroinvertebrates",
        network_desc="Data from freshwater river macroinvertebrate surveys "
                     "carried out across England from 1965 onwards.",
        folder="inv",
        shape="diamond",
        access="geojson",
        updates="Realtime data available",
        website=paths.EA_BIO_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.EA_INV_ID),
    )
    network_json.append(_to_json_format(EA_INV_dict))

    # EA MACP
    EA_MACP_dict = make_network_dict(
        network_id=config.EA_MACP_ID,
        network_name="EA Macrophyte",
        network_desc="Data from freshwater river macrophyte (plant) surveys "
                     "carried out across England from 1980 onwards.",
        folder="macp",
        shape="triangle-up",
        access="geojson",
        updates="Realtime data available",
        website=paths.EA_BIO_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.EA_MACP_ID),
    )
    network_json.append(_to_json_format(EA_MACP_dict))

    # EA DIAT
    EA_DIAT_dict = make_network_dict(
        network_id=config.EA_DIAT_ID,
        network_name="EA Diatom",
        network_desc="Data from freshwater river diatom (algae) surveys "
                     "carried out across England from 1993 onwards.",
        folder="diat",
        shape="arrowhead-down",
        access="geojson",
        updates="Realtime data available",
        website=paths.EA_BIO_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.EA_DIAT_ID),
    )
    network_json.append(_to_json_format(EA_DIAT_dict))

    # EA fish
    EA_FISH_dict = make_network_dict(
        network_id=config.EA_FISH_ID,
        network_name="National Fish Population Database (NFPD) Observations",
        network_desc="NFPD is a database for storing, manipulating and "
                     "reporting data from freshwater and transitional and "
                     "coastal water (TraC) fish monitoring surveys. These "
                     "samples are collected and analysed by the Environment "
                     "Agency and by third parties.",
        folder="fish",
        shape="triangle-down",
        access="geojson",
        updates="Realtime data available",
        website=paths.EA_FISH_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.EA_FISH_ID),
    )
    network_json.append(_to_json_format(EA_FISH_dict))

    # Riverflies
    RF_dict = make_network_dict(
        network_id=config.RF_ID,
        network_name="The Riverfly Partnership",
        network_desc="The Riverfly Partnership is a dynamic network of "
                     "organisations, representing anglers, conservationists, "
                     "entomologists, scientists, water course managers and "
                     "relevant authorities, working together to: - protect the "
                     "water quality of our rivers; - further the understanding "
                     "of riverfly populations; - and actively conserve "
                     "riverfly habitats.",
        folder="rf",
        shape="arrowhead-up",
        access="geojson",
        updates="Realtime data not available",
        website=paths.RF_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.RF_ID),
    )
    network_json.append(_to_json_format(RF_dict))

    # Fresh water watch
    FWW_dict = make_network_dict(
        network_id=config.FWW_ID,
        network_name="FreshWater Watch",
        network_desc="FreshWater Watch is a global project run by Earthwatch "
                     "Europe, in which individuals and communities monitor, "
                     "protect and restore their local water resources.",
        folder="fww",
        shape="circle",
        access="geojson",
        updates="Realtime data not available",
        website=paths.FWW_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.FWW_ID),
    )
    network_json.append(_to_json_format(FWW_dict))

    # NRFA
    FWW_dict = make_network_dict(
        network_id=config.NRFA_ID,
        network_name="UK National River Flow Archive",
        network_desc="The National River Flow Archive (NRFA) is the UK’s "
                     "focal point for river flow data",
        folder="nrfa",
        shape="x",
        access="geojson",
        updates="Realtime data available",
        website=paths.NRFA_WEBSITE,
        dtype_ids=_get_dtype_dicts(config.NRFA_ID),
    )
    network_json.append(_to_json_format(FWW_dict))



    with open(paths.METADATA_NETWORK_JSON_FILE, 'w') as f:
        json.dump(network_json, f)



# *** Convert to GeoJSON ******************************************************
def _init_geojson():
    return {
      "type": "FeatureCollection",
      "crs": {
        "type": "name",
        "properties": {
          "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
        },
      },
      "features": [],
    }


def _alt_coord_change():
    # First look for ALT_COORDS.
    alt_coord_sites = no_area_sites[
        no_area_sites["ALT_COORDS"].notnull()]
    if len(alt_coord_sites) > 0:
        for i, row in alt_coord_sites.iterrows():
            for alt_coords in row["ALT_COORDS"].split(";"):
                lat, lon = alt_coords.split(":")
                # Remove brackets and make floats.
                lat = float(lat[1:])
                lon = float(lon[:-1])

                no_area_sites.loc[
                    no_area_sites["SITE_ID"] == row["SITE_ID"],
                    "LATITUDE"] = lat
                no_area_sites.loc[
                    no_area_sites["SITE_ID"] == row["SITE_ID"],
                    "LONGITUDE"] = lon


def availability_geojson_split(networks="all", split_area="op_catchments",
                               try_round_coords=False, save_live=False):
    """
    Read in CSV availability and convert to geoJSON object. Each geoJSON
    feature is the data availability of a particular datatum from a particular
    network.

    Load in the IHU groups polygons, figure out with area each site sits in
    from that and save to separate geoJSON file.

    """
    area_jsons_dict = {
        "op_catchments": {
            "fname": "WFD_Surface_Water_Operational_Catchments_Cycle_2.json",
            "area_col": "opcat_id",
            "area_key": "opcat_id",
            "prefix": "OPCAT_",
        },
        "ihu_areas": {
            "fname": "ihu_areas.json",
            "area_col": "HA_ID",
            "area_key": "ihu_area_id",
        },
        "ihu_groups": {
            "fname": "ihu_groups.json",
            "area_col": "G_ID",
            "area_key": "ihu_group_id",
        },
    }

    if split_area not in area_jsons_dict:
        raise UserWarning("%s is not a valid split area" % split_area)

    if isinstance(networks, str):
        networks = [networks]
    if networks[0] == "all":
        networks = config.VALID_NETWORKS
    else:
        for ntwrk in networks:
            if ntwrk not in config.VALID_NETWORKS:
                raise UserWarning("%s is not a valid network. Choose from %s"
                                  % (ntwrk, ", ".join(config.VALID_NETWORKS)))

    area_fname = area_jsons_dict[split_area]["fname"]
    area_col = area_jsons_dict[split_area]["area_col"]
    area_key = area_jsons_dict[split_area]["area_key"]
    prefix = area_jsons_dict[split_area].get("prefix", "")

    # Load in json containing
    area_data = gpd.read_file("%s%s" % (paths.METADATA_AREA_JSON_DIR,
                                        area_fname),
                              driver='GeoJSON', crs=4326)

    for network_id in networks:
        # Initilise dictionary to hold separate geoJSONs for each area
        area_geojsons = {}

        # Read in data from CSVs
        data_avail = pd.read_csv(
            paths.DATA_AVAILABILITY_FPATH.format(NETWORK=network_id),
            parse_dates=["START_DATE", "END_DATE"], dtype={"SITE_ID": str,
                                                           "DTYPE_ID": str})
        data_avail = data_avail.replace({np.nan: None})

        dtypes_info = pd.read_csv(
            paths.DTYPE_REGISTER_FPATH.format(NETWORK=network_id),
            dtype={"DTYPE_ID": str})
        dtypes_info = dtypes_info.replace({np.nan: None})

        # Extract site info and join with IHU areas and groups
        sites_info = pd.read_csv(
            paths.SITE_REGISTER_FPATH.format(NETWORK=network_id),
            dtype={"SITE_ID": str})
        sites_info = sites_info.replace({np.nan: None})

        sites_info_geo = gpd.GeoDataFrame(sites_info.copy(), crs=4326,
                                          geometry=gpd.points_from_xy(
                                              sites_info["LONGITUDE"],
                                              sites_info["LATITUDE"]))

        sites_groups = sjoin(sites_info_geo, area_data, how='left')

        # Check all points have been assigned an area.
        no_area_sites = sites_groups[sites_groups[area_col].isnull()]
        if len(no_area_sites) > 0:
            print("Warning, the following sites for %s did not land in an "
                  "IHU group: %s." % (
                    network_id, no_area_sites["SITE_ID"].values))

        if try_round_coords:
                print("Rounding coords and trying again")
                # This happens when site coordinates fall on polygon boundaries.
                # Extract sites from original site_info DF
                no_area_sites = sites_info[
                    sites_info["SITE_ID"].isin(
                        no_area_sites["SITE_ID"])].copy()

                no_area_sites = no_area_sites.round({"LATITUDE": 1,
                                                     "LONGITUDE": 2})

                no_area_geo = gpd.GeoDataFrame(no_area_sites.copy(),
                                               crs=4326,
                                               geometry=gpd.points_from_xy(
                                                   no_area_sites["LONGITUDE"],
                                                   no_area_sites["LATITUDE"]))
                no_area_groups = sjoin(no_area_geo, area_data, how='left')

                if len(no_area_groups[no_area_groups[area_col].isnull()]) > 0:

                    print("Error, rounding coordinates did not work for: %s. "
                          "These sites will be ignored. Please investigate."
                          % no_area_groups[
                                no_area_groups[area_col].isnull()][
                                    "SITE_ID"].values)
                else:
                    print("Rounding coordinates worked for all sites. Be "
                          "aware could lead to sites being placed in the "
                          "wrong group.")

                sites_groups = sites_groups.combine_first(no_area_groups)

        sites_groups = sites_groups[sites_groups[area_col].notnull()]

        # Sort case where ID are numbers and pandas switches to floats not ints
        if sites_groups[area_col].dtype == "float64":
            sites_groups[area_col] = sites_groups[area_col].astype(int).astype(str)

        # We join multiple data type availaibility info into a single site
        # feature dictionary, so create a site reference dict.
        site_feat_dict = {}
        total_rows = len(data_avail)
        for i, row in data_avail.iterrows():
            print("%s avail row: %s / %s" % (network_id, i, total_rows))
            site_id = row["SITE_ID"]
            # Extract site and data type info, making sure there is only one
            # entry found for each.
            site = sites_groups[
                (sites_groups["SITE_ID"] == site_id) &
                (sites_groups["NETWORK_ID"] == row["NETWORK_ID"])]

            if len(site) != 1:
                if len(site) == 0:
                    continue
                else:
                    raise UserWarning("Multiple site info found for %s"
                                      % site_id)
            else:
                site = site.iloc[0]

            dtype = dtypes_info[(dtypes_info["DTYPE_ID"] == row["DTYPE_ID"]) &
                                (dtypes_info["NETWORK_ID"] == row["NETWORK_ID"])]
            if len(dtype) != 1:
                if len(dtype) == 0:
                    raise UserWarning("No data type info found for %s"
                                      % row["DTYPE_ID"])
                else:
                    raise UserWarning("Multiple data type info found for %s"
                                      % row["DTYPE_ID"])
            else:
                dtype = dtype.iloc[0]

            dtype_dict = {
                "dtype_id": dtype["DTYPE_ID"],
                "dtype_name": dtype["DTYPE_NAME"],
                "dtype_desc": dtype["DTYPE_DESC"],
                "start_date": row["START_DATE"].strftime(config.DATE_FORMAT),
                "end_date": row["END_DATE"].strftime(config.DATE_FORMAT),
                "value_count": row["SITE_VALUE_COUNT"],
                "value_mean": row["SITE_VALUE_MEAN"],
            }

            if site_id not in site_feat_dict:
                feature = {
                    "type": "Feature",
                    "properties": {
                        "site_id": site_id,
                        "site_name": site["SITE_NAME"],
                        "network_id": row["NETWORK_ID"],
                        area_key: site[area_col],
                        "dtypes": [dtype_dict],
                        "dtype_count": 1,
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [site["LONGITUDE"], site["LATITUDE"]]
                    },
                }
                site_feat_dict[site_id] = feature
            else:
                # Site dict exists, just add dtype dictionary.
                site_feat_dict[site_id]["properties"]["dtypes"].append(
                    dtype_dict)
                site_feat_dict[site_id]["properties"]["dtype_count"] += 1

        # Separate by area
        for site_id, site_dict in site_feat_dict.items():
            area = site_dict["properties"][area_key]

            if area not in area_geojsons:
                area_geojsons[area] = _init_geojson()

            area_geojsons[area]["features"].append(site_dict)

        if save_live:
            save_path = paths.SAN_AVAIL_JSON_DIRS[network_id]
        else:
            save_path = paths.METADATA_AVAIL_JSON_DIR

        # Save each area in separate JSON file
        for area, geojson in area_geojsons.items():
            fpath = "%s%s%s_%s_availability.json" % (save_path,
                                                     prefix,
                                                     area,
                                                     network_id)

            with open(fpath, 'w') as f:
                json.dump(geojson, f)


def data_types_json(networks="all"):
    """
    Save network datype dictionaries

    """
    if isinstance(networks, str):
        networks = [networks]
    if networks[0] == "all":
        networks = config.VALID_NETWORKS
    else:
        for ntwrk in networks:
            if ntwrk not in config.VALID_NETWORKS:
                raise UserWarning("%s is not a valid network. Choose from %s"
                                  % (ntwrk, ", ".join(config.VALID_NETWORKS)))

    for network_id in networks:
        dtype_dicts = _get_dtype_dicts(network_id)
        fpath = "%s%s_data_types.json" % (paths.METADATA_DTYPE_JSON_DIR,
                                          network_id)

        with open(fpath, 'w') as f:
            json.dump(dtype_dicts, f)


if __name__ == "__main__":

    #create_EA_WQ_metadata()
    #create_all_EA_BIO_metadata()
    #create_EA_FISH_metadata()
    #create_RF_metadata()
    #create_FWW_metadata()
    #create_NRFA_metadata()

    #create_all_metadata_csv()

    #create_network_json()
    availability_geojson_split(split_area="op_catchments")
