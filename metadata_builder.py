# -*- coding: utf-8 -*-
"""
Functions to create the site and data availability metadata files for the
following networks:

    - EA water quality (EA_WQ)
    - EA Biosys data (EA_BIO)
    - EA fish data (EA_FISH)
    - EA additional gauging sites (EA_GS)
    - Riverfly survey (RS)
    - FreshwaterWatch (FWW)
    - GRTS monitoring locations (GRTS)

The metadata files are:
    - site_register
    - data_type_register
    - data_avialability

There is one of these for each network.

The files created by these functions are written in a stucture understandable
by functions in metadata_query.py
They have the following structures:

site_register
SITE_ID, SITE_NAME, NETWORK, LATITUDE, LONGITUDE, ALT_COORDS

data_type_register
DTYPE_ID, NETWORK, DTYPE_DESC

data_availability
SITE_ID, NETWORK, DTYPE_ID, START_DATE, END_DATE, VALUE_COUNT

"""
import paths
import config

import urllib.request
import json
import pandas as pd
import geopandas as gpd

from geopandas.tools import sjoin
from datetime import datetime
from pyproj import Transformer


def site_dict(site_id=None, site_name=None, network=None, lat=None,
              long=None, alt_coords=None):
    """
    Create site_register dictionary. Equates to a row in the metadata file.

    """
    return {
        "SITE_ID": site_id,
        "SITE_NAME": site_name,
        "NETWORK": network,
        "LATITUDE": lat,
        "LONGITUDE": long,
        "ALT_COORDS": alt_coords,
    }


def dtype_dict(dtype_id=None, network=None, dtype_desc=None):
    """
    Create dtype_register dictionary. Equates to a row in the metadata file.

    """
    return {
        "DTYPE_ID": dtype_id,
        "NETWORK": network,
        "DTYPE_DESC": dtype_desc,
    }


def avail_dict(site_id=None, network=None, dtype_id=None, start_date=None,
               end_date=None, value_count=None):
    """
    Create data_availability dictionary. Equates to a row in the metadata file.

    """
    return {
        "SITE_ID": site_id,
        "NETWORK": network,
        "DTYPE_ID": dtype_id,
        "START_DATE": start_date,
        "END_DATE": end_date,
        "VALUE_COUNT": value_count,
    }


# *** EA water quality ********************************************************
"""
Data for EA water quality (EA_WQ) samples, is fetched from an API.

"""
def _get_EA_WQ_samples_availability(site_id, api_date_frmt):
    """
    Get the data availability of samples for this site.

    """
    limit = 500
    offset = 0

    smpls_dict = None
    finished = False
    first_call = True
    while finished is False:
        # Query string
        query = "_limit=%s&_offset=%s" % (limit, offset)
        # Full URL
        url = "%s/%s/samples?%s" % (paths.WQ_API_URL, site_id, query)

        with urllib.request.urlopen(url) as response:
            print("Samples call %s to %s for %s" % (offset, offset + limit,
                                                    site_id))
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data["items"]) < limit:
                finished = True

            for sample_info in data["items"]:
                sample_date = datetime.strptime(sample_info["sampleDateTime"],
                                                api_date_frmt)
                if smpls_dict is None:
                    smpls_dict = avail_dict(site_id=site_id,
                                           network=config.EA_WQ_ID,
                                           dtype_id="WQ_SAMPLE",
                                           start_date=sample_date,
                                           end_date=sample_date,
                                           value_count=1)


                else:
                    if sample_date < smpls_dict["START_DATE"]:
                        smpls_dict["START_DATE"] = sample_date
                    elif sample_date > smpls_dict["END_DATE"]:
                        smpls_dict["END_DATE"] = sample_date

                    smpls_dict["VALUE_COUNT"] += 1

        offset += limit

    return smpls_dict


def create_EA_WQ_metadata():
    """
    Fetch site and data availability for EA water quality (EA_WQ) data
    and save to file. Data is fetch from the live API.

    """
    api_date_frmt = "%Y-%m-%dT%H:%M:%S"

    # Add fish data type metadata ---------------------------------------------
    dtype_rows = [
        dtype_dict(dtype_id="WQ_SAMPLE",
                   network=config.EA_WQ_ID,
                   dtype_desc="Water quality samples")
    ]
    pd.DataFrame(dtype_rows).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_WQ_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    # Set up coordinate transformation
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    # Add Site metadata -------------------------------------------------------
    limit = 500
    offset = 0

    sites_rows = []
    avail_rows = []

    finished = False
    first_call = True
    while finished is False:
        # Query string
        query = "_limit=%s&_offset=%s" % (limit, offset)
        # Full URL
        url = "%s?%s" % (paths.WQ_API_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Sites call %s to %s" % (offset, offset + limit))
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data["items"]) < limit:
                finished = True

            for site_info in data["items"]:
                lat, long = transformer.transform(site_info["easting"],
                                                  site_info["northing"])

                site = site_dict(site_id=site_info["notation"],
                                 site_name=site_info["label"],
                                 network=config.EA_WQ_ID,
                                 lat=lat,
                                 long=long)

                avail_row = _get_EA_WQ_samples_availability(
                    site_info["notation"], api_date_frmt)

                if avail_row is None:
                    print("No sample data found for site %s. Not adding to "
                          "availability metadata" % site_info["notation"])
                else:
                    sites_rows.append(site)
                    avail_rows.append(avail_row)

        sites = pd.DataFrame(sites_rows)
        avail = pd.DataFrame(avail_rows)

        if first_call:
            # We want to overwrite any existing CSV
            sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         na_rep=None,
                         index=False,
                         date_format=config.DATE_FORMAT)

            avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         na_rep=None,
                         index=False,
                         date_format=config.DATE_FORMAT)

            first_call = False

        else:
            # Append to exisiting CSV
            sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         mode='a',
                         na_rep=None,
                         index=False,
                         header=False,
                         date_format=config.DATE_FORMAT)

            avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                             NETWORK=config.EA_WQ_ID),
                         mode='a',
                         na_rep=None,
                         index=False,
                         header=False,
                         date_format=config.DATE_FORMAT)

        offset += limit


# *** EA BioSys and Fish data *************************************************
"""
Data for EA Biosys (EA_BIO) and EA Fish (EA_FISH) is fetched from an API.

"""
def _extract_EA_BIO_metadata(site_info, data_types, api_date_frmt,
                             transformer):
    """
    Extract site and data availability data from the site info dictionary
    returned in the API response.

    """
    lat, long = transformer.transform(site_info["easting"],
                                      site_info["northing"])

    # Site data
    site_id = site_info["local_id"]
    site = site_dict(site_id=site_id,
                     site_name=site_info["label"],
                     network=config.EA_BIO_ID,
                     lat=lat,
                     long=long)

    # Data availability
    dtype_avail = {}
    for prop in site_info["properties"]:
        # Split label string into listed words
        prop_label_words = prop["property_label"].split()
        # We only want properties to do with a particular data
        # type, i.e. INV, MACB or DIAT. These are given as last
        # word in property_label.
        dtype_id = prop_label_words[-1]
        if dtype_id not in data_types:
            continue

        # Next, the first word tells us the property type;
        # "Min"/"Max" for min.max date, or "Count" for value count
        if prop_label_words[0] == "Min":
            # Extract and reformat date string
            start_date = datetime.strptime(prop["value"], api_date_frmt)

            # Create/add to row dictionary
            if dtype_id not in dtype_avail:
                dtype_avail[dtype_id] = avail_dict(site_id=site_id,
                                                   network=config.EA_BIO_ID,
                                                   dtype_id=dtype_id,
                                                   start_date=start_date)
            else:
                dtype_avail[dtype_id]["START_DATE"] = start_date

        elif prop_label_words[0] == "Max":
            # Extract and reformat date string
            end_date = datetime.strptime(prop["value"], api_date_frmt)

            # Create/add to row dictionary
            if dtype_id not in dtype_avail:
                dtype_avail[dtype_id] = avail_dict(site_id=site_id,
                                                   network=config.EA_BIO_ID,
                                                   dtype_id=dtype_id,
                                                   end_date=end_date)
            else:
                dtype_avail[dtype_id]["END_DATE"] = end_date

        elif prop_label_words[0] == "Count":
            # Create/add to row dictionary
            if dtype_id not in dtype_avail:
                dtype_avail[dtype_id] = avail_dict(site_id=site_id,
                                                   network=config.EA_BIO_ID,
                                                   dtype_id=dtype_id,
                                                   value_count=prop["value"])
            else:
                dtype_avail[dtype_id]["VALUE_COUNT"] = prop["value"]

    return site, list(dtype_avail.values())


def create_EA_BIO_FISH_metadata(limit_calls=None):
    """
    Fetch site and data availability for EA Biosys (EA_BIO) and fish (EA_FISH)
    data and save to file.

    Note, these are treated as separate networks but are accessed through the
    same API.

    How data is delivered through the API differs for BioSys and fish.
    For BioSys we can extract all info from the "sites" call.
    However, for fish we must use both the "sites" call and the "surveys"
    calls.

    """
    network_id_base = "http://environment.data.gov.uk/ecology/def"
    bio_ntwk_id = "%s/BiosysFreshwaterSite" % network_id_base
    fish_ntwk_id = "%s/FishFreshwaterSite" % network_id_base

    # Set API paging settings
    limit = 500
    offset = 0

    api_date_frmt = "%Y-%m-%d"

    # Create BioSys data types metadata
    bio_dtypes = {
        "INV": "Macroinvertebrates",
        "MACP": "Macrophyte",
        "DIAT": "Diatom",
    }
    bio_dtype_rows = []
    # Add BioSys data type metadata
    for dtype_id, dtype_desc in bio_dtypes.items():
        bio_dtype_rows.append(dtype_dict(dtype_id=dtype_id,
                                         network=config.EA_BIO_ID,
                                         dtype_desc=dtype_desc))

    # Save data types
    pd.DataFrame(bio_dtype_rows).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_BIO_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    # Add fish data type metadata
    fish_dtype_rows = [
        dtype_dict(dtype_id="FISH_SURVEY",
                   network=config.EA_FISH_ID,
                   dtype_desc="Fish Surveys")
    ]
    pd.DataFrame(fish_dtype_rows).to_csv(
        paths.DTYPE_REGISTER_FPATH.format(NETWORK=config.EA_FISH_ID),
        index=False,
        date_format=config.DATE_FORMAT)

    # Set up coordinate transformation
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

    fish_site_names = {}

    finished = False
    first_call = True
    while finished is False:
        bio_sites_rows = []
        bio_avail_rows = []

        # Query string
        query = "take=%s&skip=%s&mode=props" % (limit, offset)
        # Full URL
        url = "%ssites?%s" % (paths.EA_ECO_BASE_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Sites call %s to %s" % (offset, offset + limit))
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data) < limit:
                finished = True

            for site_info in data:
                if site_info["type"] == bio_ntwk_id:
                    # EA Biosys site. Extract all data fom the site_info
                    site, dtype_avail = _extract_EA_BIO_metadata(
                        site_info, bio_dtypes, api_date_frmt, transformer)

                    bio_sites_rows.append(site)
                    bio_avail_rows += dtype_avail

                elif site_info["type"] == fish_ntwk_id:
                    # EA fish site. This only contains the site name for now.
                    fish_site_names[site_info["local_id"]] = site_info["label"]

        if limit_calls is not None:
            if offset >= limit_calls:
                finished = True

        bio_sites = pd.DataFrame(bio_sites_rows)
        bio_avail = pd.DataFrame(bio_avail_rows)

        if first_call:
            # We want to overwrite any existing CSV
            bio_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             na_rep=None,
                             index=False,
                             date_format=config.DATE_FORMAT)

            bio_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             na_rep=None,
                             index=False,
                             date_format=config.DATE_FORMAT)

            first_call = False

        else:
            # Append to exisiting CSV
            bio_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             mode='a',
                             na_rep=None,
                             index=False,
                             header=False,
                             date_format=config.DATE_FORMAT)

            bio_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                                NETWORK=config.EA_BIO_ID),
                             mode='a',
                             na_rep=None,
                             index=False,
                             header=False,
                             date_format=config.DATE_FORMAT)

        offset += limit


    # Now we need to read through the survey data to extract more info on the
    # fish sites.
    offset = 0

    fish_sites_rows = {}
    fish_avail_rows = {}

    alt_coords_dict = {}

    finished = False
    first_call = True
    while finished is False:
        # Query string
        query = "take=%s&skip=%s&site_type=%s" % (
            limit, offset, fish_ntwk_id)
        # Full URL
        url = "%ssurveys?%s" % (paths.EA_ECO_BASE_URL, query)

        with urllib.request.urlopen(url) as response:
            print("Surveys call %s to %s" % (offset, offset + limit))
            data = json.load(response)

            # Check if response has less than limit, indicating the end of
            # available sites
            if len(data) < limit:
                finished = True

            for surv_info in data:
                site_id = surv_info["site_id"].split("/")[-1]
                survey_date = datetime.strptime(surv_info["survey_date"],
                                                api_date_frmt)

                lat = surv_info["survey_lat"]
                long = surv_info["survey_long"]

                # Sort site metadata
                if site_id not in fish_sites_rows:
                    site_name = fish_site_names.get(site_id)
                    if site_name is None:
                        print("No fish site name found for %s" % site_id)

                    fish_sites_rows[site_id] = site_dict(
                        site_id=site_id,
                        site_name=site_name,
                        network=config.EA_FISH_ID,
                        lat=lat,
                        long=long)
                else:
                    if fish_sites_rows[site_id]["LATITUDE"] != lat or \
                            fish_sites_rows[site_id]["LONGITUDE"] != long:
                        print("Fish site found with differing coordinates for "
                              "survey: %s" % site_id)
                        alt_coords = "(%s:%s)" % (lat, long)
                        if fish_sites_rows[site_id]["ALT_COORDS"] is None:
                            fish_sites_rows[site_id]["ALT_COORDS"] = [alt_coords]
                        elif alt_coords not in fish_sites_rows[site_id]["ALT_COORDS"]:
                            fish_sites_rows[site_id]["ALT_COORDS"].append(alt_coords)

                # Sort data availability
                if site_id not in fish_avail_rows:
                    fish_avail_rows[site_id] = avail_dict(
                        site_id=site_id,
                        network=config.EA_FISH_ID,
                        dtype_id="FISH_SURVEY",
                        start_date=survey_date,
                        end_date=survey_date,
                        value_count=1)
                else:
                    if survey_date < fish_avail_rows[site_id]["START_DATE"]:
                        fish_avail_rows[site_id]["START_DATE"] = survey_date
                    elif survey_date > fish_avail_rows[site_id]["END_DATE"]:
                        fish_avail_rows[site_id]["END_DATE"] = survey_date

                    fish_avail_rows[site_id]["VALUE_COUNT"] += 1


        if limit_calls is not None:
            if offset >= limit_calls:
                finished = True

        offset += limit

    # Switch any ALT_COORDS lists into strings
    for site_id in fish_sites_rows:
        if fish_sites_rows[site_id]["ALT_COORDS"] is not None:
            alt_coord_str = ";".join(fish_sites_rows[site_id]["ALT_COORDS"])
            fish_sites_rows[site_id]["ALT_COORDS"] = alt_coord_str

    fish_sites = pd.DataFrame(fish_sites_rows.values())
    fish_avail = pd.DataFrame(fish_avail_rows.values())

    if first_call:
        # Overwrite any existing CSV
        fish_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          na_rep=None,
                          index=False,
                          date_format=config.DATE_FORMAT)

        fish_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          na_rep=None,
                          index=False,
                          date_format=config.DATE_FORMAT)

        first_call = False

    else:
        # Append to exisiting CSV
        fish_sites.to_csv(paths.SITE_REGISTER_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          mode='a',
                          na_rep=None,
                          index=False,
                          header=False,
                          date_format=config.DATE_FORMAT)

        fish_avail.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                            NETWORK=config.EA_FISH_ID),
                          mode='a',
                          na_rep=None,
                          index=False,
                          header=False,
                          date_format=config.DATE_FORMAT)



# *** EA additional gauging sites *********************************************
"""
Data for EA additional gauging sites (GS) is fetched from a CSV. These are
site not included in NRFA.

"""




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


def availability_geojson_by_area(networks="all"):
    """
    Read in CSV availability and convert to geoJSON object. Each geoJSON
    feature is the data availability of a particular datatum from a particular
    network.

    Load in the IHU groups polygons, figure out with area each site sits in
    from that and save to separate geoJSON file.

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

    # Load in IHU areas and groups data. ihu_groups.json contains both
    ihu_groups = gpd.read_file("%sihu_groups.json" % paths.METADATA_IHU_DIR,
                               driver='GeoJSON', crs=4326)
    ihu_areas = gpd.read_file("%sihu_areas.json" % paths.METADATA_IHU_DIR,
                               driver='GeoJSON', crs=4326)

    for network in networks:
        # Initilise geoJSON dictionary
        area_geojsons = {}

        data_avail = pd.read_csv(
            paths.DATA_AVAILABILITY_FPATH.format(NETWORK=network),
            parse_dates=["START_DATE", "END_DATE"])

        dtypes_info = pd.read_csv(
            paths.DTYPE_REGISTER_FPATH.format(NETWORK=network))

        # Extract site info and join with IHU areas and groups
        sites_info = pd.read_csv(
            paths.SITE_REGISTER_FPATH.format(NETWORK=network))
        sites_info_geo = gpd.GeoDataFrame(sites_info.copy(), crs=4326,
                                          geometry=gpd.points_from_xy(
                                              sites_info["LONGITUDE"],
                                              sites_info["LATITUDE"]))
        sites_groups = sjoin(sites_info_geo, ihu_groups, how='left')

        # Check all points have been assigned an area.
        no_area_sites = sites_groups[sites_groups["HA_ID"].isnull()]
        if len(no_area_sites) > 0:
            print("Warning, the following sites for %s did not land in an "
                  "IHU group: %s. Rounding coordinates and trying again."
                  % (network, no_area_sites["SITE_ID"].values))
            # This happens when site coordinates fall on polygon boundaries.
            # Extract sites from original site_info DF
            no_area_sites = sites_info[
                sites_info["SITE_ID"].isin(no_area_sites["SITE_ID"])].copy()

            no_area_sites = no_area_sites.round({"LATITUDE": 1,
                                                 "LONGITUDE": 2})

            no_area_geo = gpd.GeoDataFrame(no_area_sites.copy(),
                                           crs=4326,
                                           geometry=gpd.points_from_xy(
                                               no_area_sites["LONGITUDE"],
                                               no_area_sites["LATITUDE"]))
            no_area_groups = sjoin(no_area_geo, ihu_groups, how='left')

            if len(no_area_groups[no_area_groups["HA_ID"].isnull()]) > 0:

                print("Error, rounding coordinates did not work for: %s. These "
                      "sites will be ignored. Please investigate."
                      % no_area_groups[
                        no_area_groups["HA_ID"].isnull()]["SITE_ID"].values)
            else:
                print("Rounding coordinates worked for all sites. Be aware "
                      "could lead to sites being placed in the wrong group.")

            sites_groups = sites_groups.combine_first(no_area_groups)

        for i, row in data_avail.iterrows():
            # Extract site and data type info, making sure there is only one
            # entry found for each.
            site = sites_groups[(sites_groups["SITE_ID"] == row["SITE_ID"]) &
                                (sites_groups["NETWORK"] == row["NETWORK"])]

            if pd.isna(site["HA_ID"]).iloc[0]:
                continue

            if len(site) != 1:
                if len(site) == 0:
                    raise UserWarning("No site info found for %s"
                                      % row["SITE_ID"])
                else:
                    raise UserWarning("Multiple site info found for %s"
                                      % row["SITE_ID"])
            else:
                site = site.iloc[0]

            dtype = dtypes_info[(dtypes_info["DTYPE_ID"] == row["DTYPE_ID"]) &
                                (dtypes_info["NETWORK"] == row["NETWORK"])]
            if len(dtype) != 1:
                if len(dtype) == 0:
                    raise UserWarning("No data type info found for %s"
                                      % row["DTYPE_ID"])
                else:
                    raise UserWarning("Multiple data type info found for %s"
                                      % row["DTYPE_ID"])
            else:
                dtype = dtype.iloc[0]

            feature = {
                "type": "Feature",
                "properties": {
                    "site_id": row["SITE_ID"],
                    "site_name": site["SITE_NAME"],
                    "network_id": row["NETWORK"],
                    "ihu_area_id": site["HA_ID"],
                    "ihu_group_id": site["G_ID"],
                    "dtype_id": dtype["DTYPE_ID"],
                    "dtype_desc": dtype["DTYPE_DESC"],
                    "start_date": row["START_DATE"].strftime(config.DATE_FORMAT),
                    "end_date": row["END_DATE"].strftime(config.DATE_FORMAT),
                    "value_count": row["VALUE_COUNT"],
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [ site["LATITUDE"], site["LONGITUDE"] ]
                },
            }

            if site["HA_ID"] not in area_geojsons:
                area_geojsons[site["HA_ID"]] = _init_geojson()

            area_geojsons[site["HA_ID"]]["features"].append(feature)

        # Save each area in separate JSON file
        for area_id, geojson in area_geojsons.items():
            fpath = "%s%s_%s_availability.json" % (paths.METADATA_GJSON_DIR,
                                                   area_id,
                                                   network)
            with open(fpath, 'w') as f:
                json.dump(geojson, f)


#create_EA_BIO_FISH_availability()
create_EA_WQ_availability()
