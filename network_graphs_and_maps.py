# -*- coding: utf-8 -*-
"""
Create JSON files for the portal graphs and maps

"""
import paths
import config
import utils

import os
import json
import pandas as pd
import geopandas as gpd

from geopandas.tools import sjoin


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


def create_RF_maps_and_graphs_data(save_live=False):
    """
    Fetch site, data availability and data type info for Riverflies (RF) data
    and save to file.

    """
    dytpe_ids = [x for x in RF_DTYPE_DICT.keys()]
    usecols = ["Site", "River", "Date", "Time", "Lat", "Long", "Action",
               "Record Score", "Threshold on date"] + dytpe_ids

    area_data = gpd.read_file(
        "%sihu_areas.json" % paths.METADATA_AREA_JSON_DIR,
        driver='GeoJSON',
        crs=4326)

    group_data = gpd.read_file(
        "%sWFD_Surface_Water_Operational_Catchments_Cycle_2.json" %
            paths.METADATA_AREA_JSON_DIR,
        driver='GeoJSON',
        crs=4326)


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

        rf_data = rf_data.rename(columns={
            "Record Score": "Record_Score",
            "Threshold on date": "Threshold",
        })

        for i, row in rf_data.iterrows():
            if row["Action"] == '2nd sample on or above trigger level':
                rf_data.at[i, 'trigger_level'] = 1
            elif row["Action"] =='Historic Record (no Alerts or Thresholds available)':
                rf_data.at[i, 'trigger_level'] = 2
            elif row["Action"] =='Non-polluting breach':
                rf_data.at[i, 'trigger_level'] = 3
            elif row["Action"] =='Trigger breach confirmed statutory body':
                rf_data.at[i, 'trigger_level'] = 4
            elif row["Action"] =='Trigger breach NOT confirmed statutory body':
                rf_data.at[i, 'trigger_level'] = 5

            if row["Threshold"] == "" or pd.isnull(row["Threshold"]):
                rf_data.at[i,'threshold_marker_val'] = 0
            elif row["Record_Score"] < row["Threshold"]:
                rf_data.at[i,'threshold_marker_val'] = 1
            elif row["Record_Score"] > row["Threshold"]:
                rf_data.at[i,'threshold_marker_val'] = 2

        rf_data["Site_full"] = rf_data["Site"] + rf_data["River"] + \
                               rf_data["Lat"].astype(str) + \
                               rf_data["Long"].astype(str)

        rf_data["Site_id"] = rf_data["Site_full"].apply(utils._md5_hash)
        rf_data.drop('Site_full', axis=1, inplace=True)

        if rf_data["Date"].dtypes != "datetime64[ns]":
            rf_data['Date'] = pd.to_datetime(rf_data['Date'])

        rf_data["Date"] = rf_data["Date"].dt.strftime('%Y-%m-%d')

        rf_data_grouped = rf_data.groupby(
            [rf_data["Lat"], rf_data["Long"], rf_data["Site"],
             rf_data["River"], rf_data["Site_id"]], as_index=False).agg(list)

        samplesDF = gpd.GeoDataFrame(rf_data_grouped, crs='epsg:4326',
                                     geometry=gpd.points_from_xy(
                                        rf_data_grouped.Long,
                                        rf_data_grouped.Lat))

        mapColumns = ["Site_id", "Site", "River", "Lat", "Long", "Date",
                      "threshold_marker_val", "Record_Score"]

        areaMapColumns = mapColumns + ["HA_ID"]
        pointInPolys_A = sjoin(samplesDF, area_data, how="left")
        pointInPolys_A = pointInPolys_A[pointInPolys_A["HA_ID"].notnull()]
        areaMapPoints = pointInPolys_A[areaMapColumns].copy()

        groupMapColumns = mapColumns + ["opcat_id"]
        pointInPolys_G = sjoin(samplesDF, group_data, how="left")
        pointInPolys_G = pointInPolys_G[pointInPolys_G["opcat_id"].notnull()]
        groupMapPoints = pointInPolys_G[groupMapColumns].copy()

        HA_ids = areaMapPoints["HA_ID"].unique()
        group_ids = groupMapPoints["opcat_id"].unique().astype("int")

        if save_live:
            save_path = paths.SAN_MAPS_JSON_DIRS[config.RF_ID]
        else:
            save_path = paths.METADATA_MAPS_JSON_DIR

        opcat_id_grouped = groupMapPoints.groupby(groupMapPoints.opcat_id)

        for opcat_id in group_ids:
            try:
                group = opcat_id_grouped.get_group(opcat_id)

                csv_fpath = "%s%s_%s.csv" % (save_path, opcat_id, config.RF_ID)
                if os.path.exists(csv_fpath):
                    saved_group = pd.read_csv(csv_fpath)
                    group = pd.concat([saved_group, group])

                group.to_csv(csv_fpath, header=True, index=False,
                             float_format="%.4f")

                jsonStr = group.to_json(orient="records")

                json_fpath = "%s%s_%s.json" % (save_path, opcat_id,
                                               config.RF_ID)
                tfile = open(json_fpath, "w")
                tfile.write(jsonStr)
                tfile.close()
            except Exception as e:
                print(e)
                print("opcat_id error", opcat_id)
                continue

        h_ID_grouped = areaMapPoints.groupby(areaMapPoints.HA_ID)
        for HA in HA_ids:
            try:
                group = h_ID_grouped.get_group(HA)

                csv_fpath = "%s%s_%s.csv" % (save_path, HA, config.RF_ID)
                if os.path.exists(csv_fpath):
                    saved_group = pd.read_csv(csv_fpath)
                    group = pd.concat([saved_group, group])

                group.to_csv(csv_fpath, header=True, index=False,
                             float_format="%.4f")

                jsonStr = group.to_json(orient="records")

                json_fpath = "%s%s_%s.json" % (save_path, HA, config.RF_ID)
                tfile = open(json_fpath, "w")
                tfile.write(jsonStr)
                tfile.close()
            except Exception as e:
                print(e)
                print("HA_ID error",HA)
                continue

        # now create a json file for each group_id
        graphColumns = ["Site_id", "Date", "trigger_level",
                        "Threshold"] + dytpe_ids
        graphDF = rf_data_grouped[graphColumns].copy()

        if save_live:
            save_path = paths.SAN_GRAPHS_JSON_DIRS[config.RF_ID]
        else:
            save_path = paths.METADATA_GRAPHS_JSON_DIR

        for index, row in graphDF.iterrows():
            jsonStr = row.to_json()
            json_fpath = "%s%s_%s.json" % (save_path, str(row["Site_id"]),
                                           config.RF_ID)
            tfile = open(json_fpath, "w")
            tfile.write(jsonStr)
            tfile.close()


if __name__ == "__main__":
    create_RF_maps_and_graphs_data()
