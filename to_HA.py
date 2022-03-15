# -*- coding: utf-8 -*-
"""
This version uses IHU groups everywhere except in the Thames region where the
IHU group HA_039 is split into its areas

"""
import paths

import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas.tools import sjoin
import csv
import json
import requests

input_directory = paths.RAW_DATA_DIR
output_directory = paths.OUTPUT_DIR
wimsDirectory = 'C:/data/wims_data/output/determinands'
AreasDF = gpd.read_file("%sihu_areas_nc.json" % paths.METADATA_IHU_DIR,
                        driver='GeoJSON', crs={'init': 'epsg:4326'})
GroupsDF = gpd.read_file("%sspi-ihu-groups.json"  % paths.METADATA_IHU_DIR,
                         driver='GeoJSON', crs={'init': 'epsg:4326'})
print(AreasDF.columns)
print(GroupsDF.columns)

# Riverfly extraction - thought i'd see if any more areas are available outside
# of the thames, only 038 and 042 for now.
# This now exports the map and graph data separately.
with open(input_directory + "2020_Riverflies-edit.csv", mode="r") as fr:
    csvFile = pd.read_csv(fr, parse_dates=['Date']);
    csvFile['Date'] = csvFile['Date'].dt.strftime('%Y-%m-%d')
    csvFile.sort_values(by=['Date'], inplace=True, ascending=False)
    dataDF = csvFile.copy()
    dataDF_grouped = dataDF.groupby(
        [dataDF.Lat,dataDF.Long, dataDF.Site, dataDF.River],
        as_index=False).agg(list)
    dataDF_grouped['group_id'] = dataDF_grouped.index

    samplesDF = gpd.GeoDataFrame(dataDF_grouped,
                                 crs={'init': 'epsg:4326'},
                                 geometry=gpd.points_from_xy(
                                    dataDF_grouped.Long, dataDF_grouped.Lat))

    mapColumns = ['group_id', 'Site', 'River', 'Lat', 'Long', 'Date',
                  'threshold_marker_val', 'Record_Score', 'HA_ID']

    pointInPolys_A = sjoin(samplesDF, AreasDF, how='left')
    areaMapPoints = pointInPolys_A[mapColumns].copy()

    mapColumns.append('G_ID')
    pointInPolys_G = sjoin(samplesDF, GroupsDF, how='left')
    groupMapPoints = pointInPolys_G[mapColumns].copy()

    HA_ids = areaMapPoints['HA_ID'].unique()
    group_ids = groupMapPoints['G_ID'].unique()

    g_id_grouped = groupMapPoints.groupby(groupMapPoints.G_ID)

    for G_ID in group_ids:
        try:
            group = g_id_grouped.get_group(G_ID)
            group.to_csv(output_directory + 'riverflies/maps/' + str(G_ID) + '.csv',
                         header=True, index=False, float_format='%.4f')
            jsonStr = group.to_json(orient='records')
            #print(jsonStr)
            tfile = open(output_directory +'riverflies/maps/'+ str(G_ID) +'.json', 'w')
            tfile.write(jsonStr)
            tfile.close()
        except Exception as e:
            print(e)
            print('G_ID error',G_ID)
            continue

    h_ID_grouped = areaMapPoints.groupby(areaMapPoints.HA_ID)
    for HA in HA_ids:
        if HA != 'HA039':
            try:
                group = h_ID_grouped.get_group(HA)
                group.to_csv(output_directory +'riverflies/maps/'+ str(HA) +'.csv', header=True, index = False, float_format='%.4f')
                jsonStr = group.to_json(orient='records')
                #print(jsonStr)
                tfile = open(output_directory +'riverflies/maps/'+ str(HA) +'.json', 'w')
                tfile.write(jsonStr)
                tfile.close()
            except Exception as e:
                print(e)
                print('HA_ID error',HA)
                continue

    # now create a json file for each group_id
    graphColumns = ['group_id','Date','trigger_level','Threshold','Cased_caddisfly_score','Cased_caddisfly_Num','Caseless_caddisfly_score','Caseless_caddisfly_Num','Ephemeridae_score','Ephemeridae_Num','Ephemerellidae_score','Ephemerellidae_Num','Heptageniidae_score','Heptageniidae_Num','Baetidae_score','Baetidae_Num','Stoneflies_score','Stoneflies_Num','Gammarus_score','Gammarus_Num'
]
    graphDF = dataDF_grouped[graphColumns].copy()
    #print(graphDF)
    for index, row in graphDF.iterrows():
        jsonStr = row.to_json()
        tfile = open(output_directory +'riverflies/graphs/'+ str(row['group_id']) +'.json', 'w')
        tfile.write(jsonStr)
        tfile.close()

#split up bloomin algae data as a test. OBSOLETE now since elastic search can query by polygon?
with open(input_directory + "/bloomin-algae-records.json", mode="r") as fr:
    jsonDF = pd.read_json(fr)
    json_struct = json.loads(jsonDF.to_json(orient="records"))
    baDF = pd.json_normalize(json_struct)
    baDF[['lat','lng']] = baDF["data.entered_sref"].str.split(", ",expand=True).apply(lambda x: pd.to_numeric(x, downcast='float', errors='coerce'))
    #print(baDF.head(2))
    baGDF = gpd.GeoDataFrame(baDF, crs={'init': 'epsg:4326'}, geometry=gpd.points_from_xy(baDF['lng'],baDF['lat']))
    pointInPolys_A = sjoin(baGDF, AreasDF, how='left')
    print(pointInPolys_A.columns)
    HA_ids = pointInPolys_A['HA_ID'].unique()
    mapPointsDF = pointInPolys_A[['data.occurrence_id', 'data.location_name', 'data.date_start', 'data.pass', 'data.fails', 'data.query','geometry', 'HA_ID']].copy()
    HA_id_grouped = mapPointsDF.groupby(mapPointsDF.HA_ID)

    for HA in HA_ids:
        if HA != 'HA039':
            try:
                group = HA_id_grouped.get_group(HA)
                #print(group)
                jsonStr = group.to_json()
                #print(jsonStr)
                tfile = open(output_directory +'bloominAlgae/'+ str(HA) +'.json', 'w')
                tfile.write(jsonStr)
                tfile.close()
            except Exception as e:
                print(e)
                print('HA_ID error',HA)
                continue


#FWW data - concat old and new data for use later.
# convert sample_date columns to YYYY-MM-DD first
fwwColumns = ["sample_ID","sample_date","group_ID","site_name","lat","lng","freshwater_body_type","Nitrate","Phosphate"]
dfPre2020 = pd.read_csv(input_directory + "FWW-simple.csv", parse_dates=['sample_date']);
df2020Data = pd.read_csv(input_directory + "2020_waterblitzdata.csv", parse_dates=['sample_date']);
df2021SpringData = pd.read_csv(input_directory + "fww-dataset-may-18-2021.csv", parse_dates=['sample_date']);
df2021AutumnData = pd.read_csv(input_directory + "fww-dataset-oct-5-2021.csv", parse_dates=['sample_date']);
fwwDF = pd.concat([dfPre2020, df2020Data, df2021SpringData, df2021AutumnData])
#fwwDF['sample_date'] = fwwDF['sample_date'].dt.strftime('%Y-%m-%d')
fwwSamplesGDF = gpd.GeoDataFrame(fwwDF[fwwColumns], crs='epsg:4326', geometry=gpd.points_from_xy(fwwDF.lng, fwwDF.lat))

#FFW extraction
#everything except Thames HA - 039
fwwSamplesGDF= fwwSamplesGDF[fwwSamplesGDF.is_valid]
fwwSamplesGDF = fwwSamplesGDF.sort_values('sample_ID')
print(fwwSamplesGDF.tail())
AreasDF= AreasDF[AreasDF.is_valid]
pointInPolys = sjoin(fwwSamplesGDF, AreasDF, how='right')
print(pointInPolys.head())

HA_ids = pointInPolys['HA_ID'].unique()
#print(HA_ids)
#recreate geojson so only available polygons are included and the geometry is simplified.
mask = AreasDF['HA_ID'].isin(HA_ids)
geojson = AreasDF.loc[mask].copy()
#print(geojson.columns)

#AllBut38geojson = AreasDF[AreasDF['HA_ID']!='HA039'].copy()

geojson["geometry"] = geojson.geometry.simplify(tolerance=0.01,preserve_topology=False)
print(geojson.iloc[0])
geojson.to_file(input_directory + 'ihu_areas.json', driver='GeoJSON')


#create a FWW file per HA
grouped = pointInPolys.groupby(pointInPolys.HA_ID)
for HA in HA_ids:
    if HA != 'HA039':
        try:
            group = grouped.get_group(HA)
            group.to_csv(output_directory +'HA/'+ str(HA) +'.csv', header=True, index = False, float_format='%.4f',
                      columns=['sample_ID','sample_date','group_ID','site_name','lat','lng','freshwater_body_type','Nitrate','Phosphate'])
        except:
            print(HA)
            continue

#export geojson of thames groups for map
thames_groups = GroupsDF[GroupsDF['HA_ID']=='HA039']
thames_groups.reset_index(drop=True, inplace=True)
#print(HAs.columns)
thames_groups2 = thames_groups[['RIVER', 'G_NAME', 'G_ID', 'HA_ID', 'geometry']].copy()
thames_groups2.reset_index(drop=True, inplace=True)
print(thames_groups2)
thames_groups2.to_file(output_directory + "HA039.json", driver='GeoJSON')
thames_groups.to_csv(output_directory + 'HA039.csv', header=True, index = False, float_format='%.4f')


#only thames groups for FWW
pointInPolys = sjoin(fwwSamplesGDF, thames_groups, how='left')
print(pointInPolys.head())

GA_ids = pointInPolys['G_ID'].unique()
print(GA_ids)
grouped = pointInPolys.groupby(pointInPolys.G_ID)
for GA in GA_ids:
    try:
        group = grouped.get_group(GA)
        #print(group.columns)
        filteredGroup = group[['sample_ID','sample_date','group_ID','site_name','lat','lng','freshwater_body_type','Nitrate','Phosphate']].copy()
        #filteredGroup.to_json(output_directory +'v1/HAs/'+ str(HA) +'.json', orient='records')
        group.to_csv(output_directory +'GA/'+ str(GA) +'.csv', header=True, index = False, float_format='%.4f',
                  columns=['sample_ID','sample_date','group_ID','site_name','lat','lng','freshwater_body_type','Nitrate','Phosphate'])
    except:
        print(GA)
        continue

# Split NRFA stations into Areas/Groups
url = "https://nrfaapps.ceh.ac.uk/nrfa/ws/station-info?station=*&format=json-object&fields=id,name,lat-long,river,opened,closed,nhmp,live-data"
response = requests.get(url)
df = pd.read_json(response.text);
json_struct = json.loads(df.to_json(orient="records"))
df_flat = pd.json_normalize(json_struct)
columnRenames = {"data.id": "id", "data.name": "name", "data.river":"river"}
df_renamed = df_flat.rename(columns=columnRenames)
print(df_renamed.head(10))
print('len df_renamed', len(df_renamed))
df_open = df_renamed.query('`data.closed`==None' and ('`data.live-data`==True' or '`data.nhmp`==True')) #<< need recent data!
print(df_open.head(10))
print('len df_open', len(df_open))
nrfaGeoDF = gpd.GeoDataFrame(df_open, crs={'init': 'epsg:4326'}, geometry=gpd.points_from_xy(df_open['data.lat-long.longitude'],df_open['data.lat-long.latitude']))
#print(nrfaGeoDF.columns)
GroupsDF2 = GroupsDF[GroupsDF['HA_ID'] == 'HA039']

pointInPolys_A = sjoin(nrfaGeoDF, AreasDF, how='left')
pointInPolys_G = sjoin(nrfaGeoDF, GroupsDF2, how='left')

#print(pointInPolys_A.columns)
HA_ids = pointInPolys_A['HA_ID'].unique()
group_ids = pointInPolys_G['G_ID'].unique()

#print(group_ids)
#print(HA_ids)
g_id_grouped = pointInPolys_G.groupby(pointInPolys_G.G_ID)
columns=['id','name','geometry','river']

for G_ID in group_ids:
    try:
        group = g_id_grouped.get_group(G_ID)
        filteredGroup = group[columns].copy()
        filteredGroup.to_file(output_directory + "nrfa/"+str(G_ID)+".json", driver='GeoJSON')
    except Exception as e:
        print(e)
        print('error', G_ID)
        continue

h_ID_grouped = pointInPolys_A.groupby(pointInPolys_A.HA_ID)
for HA in HA_ids:
    if HA != 'HA039':
        try:
            group = h_ID_grouped.get_group(HA)
            filteredGroup = group[columns].copy()
            filteredGroup.to_file(output_directory + "nrfa/"+str(HA)+".json", driver='GeoJSON')
        except Exception as e:
            print(e)
            print('error', HA)
            continue

import pdb; pdb.set_trace()



#split nitrate areas into hydrometric groups
dfLakes = gpd.read_file(wimsDirectory + "/lake_sites_det_39_117.geojson")
dfRivers = gpd.read_file(wimsDirectory + "/riv_sites_det_39_117.geojson")
nitrateDF = pd.concat([dfLakes,dfRivers])
#print(nitrateDF.head())
pointInPolys = sjoin(nitrateDF, GroupsDF, how='left')
pointInPolys = pointInPolys.rename(columns={"G_ID_left":"G_ID"})
GA_ids = pointInPolys['G_ID'].unique()
print(GA_ids)
grouped = pointInPolys.groupby(pointInPolys.G_ID)
for GA in GA_ids:
    try:
        group = grouped.get_group(GA)
        #print(group.columns)
        filteredGroup = group[['site_id', 'site_name', 'easting', 'northing', 'material', 'det_id',
       'det_label', 'det_desc', 'num_measurements', 'min_year', 'max_year',
       'mean_measurements', 'region', 'HA_NUM_left', 'lon', 'lat', 'geometry']].copy()
        filteredGroup.to_file(output_directory + "EA_nitrate/"+GA+".json", driver='GeoJSON')
    except:
        print('error',GA)
        continue

#split phosphate areas into hydrometric groups
dfLakes = gpd.read_file(wimsDirectory + "/lake_sites_det_39_180.geojson")
dfRivers = gpd.read_file(wimsDirectory + "/riv_sites_det_39_180.geojson")
phosphateDF = pd.concat([dfLakes,dfRivers])
#print(len(phosphateDF))
pointInPolys = sjoin(phosphateDF, GroupsDF, how='left')
pointInPolys = pointInPolys.rename(columns={"G_ID_left":"G_ID"})
#print(pointInPolys.head())
GA_ids = pointInPolys['G_ID'].unique()
#print(GA_ids)
grouped = pointInPolys.groupby(pointInPolys.G_ID)
for GA in GA_ids:
    try:
        group = grouped.get_group(GA)
        #print(group.columns)
        filteredGroup = group[['site_id', 'site_name', 'easting', 'northing', 'material', 'det_id',
       'det_label', 'det_desc', 'num_measurements', 'min_year', 'max_year',
       'mean_measurements', 'region', 'HA_NUM_left', 'lon', 'lat', 'geometry']].copy()
        filteredGroup.to_file(output_directory + "EA_phosphate/"+GA+".json", driver='GeoJSON')
    except:
        print('error',GA)
        continue
