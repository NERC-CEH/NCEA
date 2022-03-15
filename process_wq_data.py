EIP_API_URL# -*- coding: utf-8 -*-
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import os

import geopandas
import geopandas.tools
from shapely.geometry import Point
from shapely.geometry import Polygon
import pyproj
from pyproj import Transformer
import time

import networkx as nx
from shapely.ops import nearest_points

import dateutil.parser
import requests
import json


def raw_data_fpath(year):
    return paths.EA_WQ_WIMS_RAW_DIR + str(year) + ".csv"


def download_latest_batch_files():
    # Download all files from EA server to 'raw' folder. Don't need to do this
    # every time, only when the raw data is updated.
    url = "%s?year=" % paths.WQ_BATCH_URL
    for year in range(2000, datetime.now().year):
        try:
            r = requests.get(url + str(year))
            open(raw_data_fpath(year), 'wb').write(r.content)
        except Exception as e:
            print(year, 'failed', e)



# Define the column names which will be required for data frames
cols = ['site_id', 'site_name', 'easting', 'northing', 'material', 'det_id',
        'det_label', 'det_desc', 'num_measurements','mean_measurements',
        'total_measurements']
cols2 = ['site_id', 'site_name', 'easting', 'northing', 'material', 'det_id',
         'det_label', 'det_desc', 'min_year', 'max_year', 'num_measurements',
         'mean_measurements', 'region']
cols3 = ['site_id','site_name','easting','northing', 'material', 'num_samples']

dfAllRivLakeSiteDets = None
dfAllGWSiteDets = None
dfAllRivLakeSiteSamps = None
dfAllRivLakeData = None

# Loop through years of data from 2000
start = time.time()
percentiles = pd.DataFrame()

for year in range(2000, datetime.now().year):
    print(year)

    # Read data file
    df = pd.read_csv(raw_data_fpath(year))

    # Subset to get river+lake sites only
    dfRivLakeData = df[
        (df['sample.sampledMaterialType.label'] == 'RIVER / RUNNING SURFACE WATER') |
        (df['sample.sampledMaterialType.label'] == 'POND / LAKE / RESERVOIR WATER')]

    # Groundwater data - note this is approximate only as it's not currrently
    # used for UKSCAPE the code doesn't aggregate samples or resolve issues
    # with different E+Ns for a single site.
    dfGWData = df[
        (df['sample.sampledMaterialType.label'] == 'GROUNDWATER') |
        (df['sample.sampledMaterialType.label'] == 'GROUNDWATER - PURGED/PUMPED/REFILLED') |
        (df['sample.sampledMaterialType.label'] == 'GROUNDWATER - STATIC/UNPURGED')]

    df = None

    # Convert sample data to proper date.
    dfRivLakeData['sample_date'] = pd.to_datetime(
        dfRivLakeData['sample.sampleDateTime'], format='%Y-%m-%dT%H:%M:%S')
    dfGWData['sample_date'] = pd.to_datetime(
        dfGWData['sample.sampleDateTime'], format='%Y-%m-%dT%H:%M:%S')

    # Group data for river+lake sites by site, determinand, and purpose code,
    # summarising number of measurements, mean of measurement, total
    # measurement and year
    gtemp = dfRivLakeData.groupby(
        ['sample.samplingPoint.notation', 'sample.samplingPoint.label',
         'sample.samplingPoint.easting', 'sample.samplingPoint.northing',
         'sample.sampledMaterialType.label', 'determinand.notation',
         'determinand.label', 'determinand.definition'])

    dfRivLakeSiteDets = gtemp.agg({
        '@id': 'count',
        'result': ['mean', 'sum']
    })
    dfRivLakeSiteDets = dfRivLakeSiteDets.reset_index()
    dfRivLakeSiteDets.columns = cols
    dfRivLakeSiteDets['year'] = year

    # And for GW data
    gtemp = dfGWData.groupby(
        ['sample.samplingPoint.notation', 'sample.samplingPoint.label',
         'sample.samplingPoint.easting', 'sample.samplingPoint.northing',
         'sample.sampledMaterialType.label', 'determinand.notation',
         'determinand.label', 'determinand.definition'])

    dfGWSiteDets = gtemp.agg({
        '@id': 'count',
        'result': ['mean', 'sum']
    })
    dfGWSiteDets = dfGWSiteDets.reset_index()
    dfGWSiteDets.columns = cols
    dfGWSiteDets ['year'] = year


    # Group annual data by site and sample date in order to count the number of
    # samples in this year.
    gtemp = dfRivLakeData.groupby(
        ['sample.samplingPoint.notation', 'sample.samplingPoint.label',
         'sample.samplingPoint.easting', 'sample.samplingPoint.northing',
         'sample.sampledMaterialType.label', 'sample_date'])

    dfRivLakeSiteSamps = gtemp.agg({
        '@id': 'count'
    })
    dfRivLakeSiteSamps = dfRivLakeSiteSamps.reset_index()

    # Group again by site, counting the number of individual dates
    gtemp = dfRivLakeSiteSamps.groupby(
        ['sample.samplingPoint.notation', 'sample.samplingPoint.label',
         'sample.samplingPoint.easting', 'sample.samplingPoint.northing',
         'sample.sampledMaterialType.label'])
    dfRivLakeSiteSamps = gtemp.agg({
        'sample_date': 'count'})
    dfRivLakeSiteSamps = dfRivLakeSiteSamps.reset_index()
    dfRivLakeSiteSamps.columns = cols3
    dfRivLakeSiteSamps['year'] = year

    # Aggregate this grouped data into one big data frame for all years
    if dfAllRivLakeSiteDets is None:
        dfAllRivLakeSiteDets = dfRivLakeSiteDets
    else:
        dfAllRivLakeSiteDets = pd.concat([dfAllRivLakeSiteDets,
                                          dfRivLakeSiteDets])
    dfRivLakeSiteDets = None

    if dfAllGWSiteDets is None:
        dfAllGWSiteDets = dfGWSiteDets
    else:
        dfAllGWSiteDets = pd.concat([dfAllGWSiteDets, dfGWSiteDets])
    dfGWSiteDets = None

    if dfAllRivLakeSiteSamps is None:
        dfAllRivLakeSiteSamps = dfRivLakeSiteSamps
    else:
        dfAllRivLakeSiteSamps = pd.concat ([dfAllRivLakeSiteSamps,
                                            dfRivLakeSiteSamps])
    dfRivLakeSiteSamps = None

    print(time.time() - start)

# Group this by site, det, and purp (?)
gtemp = dfAllRivLakeSiteDets.groupby(
    ['site_id', 'site_name', 'easting', 'northing', 'material', 'det_id',
     'det_label', 'det_desc'])
dfRivLakeSiteDets = gtemp.agg({
    'year': ['min' ,'max'],
    'num_measurements': 'sum',
    'total_measurements':  'sum'})
dfRivLakeSiteDets = dfRivLakeSiteDets.reset_index()
dfRivLakeSiteDets['mean_measurement'] = \
    dfRivLakeSiteDets['total_measurements'] / dfRivLakeSiteDets['num_measurements']
dfRivLakeSiteDets = dfRivLakeSiteDets.drop('total_measurements', axis=1)
dfRivLakeSiteDets['region'] = dfRivLakeSiteDets.site_id.str[0:2]
dfRivLakeSiteDets.columns = cols2

# Group sample data by site and sum up number of samples per site
gtemp = dfAllRivLakeSiteSamps.groupby(
    ['site_id', 'site_name', 'easting', 'northing', 'material'])
dfRivLakeSiteSamps = gtemp.agg ({'num_samples': 'sum'})
dfRivLakeSiteSamps = dfRivLakeSiteSamps.reset_index()

# Remove duplicate sites with same ID but multiple E+N that are different by
# more than 50m (keep those  where the difference is less than 50m).
gtemp = dfRivLakeSiteDets.groupby('site_id')
# Keep the number of measurements in case it's needed in decisions about sites
# to get rid of.
dfsiteCheck = gtemp.agg({
    'num_measurements': 'sum',
    'easting': ['max', 'min'],
    'northing': ['max', 'min']
})
dfsiteCheck.columns = ['meas', 'e-max', 'e-min', 'n-max', 'n-min']
dfsiteCheck['e-diff'] = dfsiteCheck['e-max'] - dfsiteCheck['e-min']
dfsiteCheck['n-diff'] = dfsiteCheck['n-max'] - dfsiteCheck['n-min']
site_good = dfsiteCheck[
    (dfsiteCheck['e-diff'] < 100) & (dfsiteCheck['n-diff'] < 100)]
site_bad = dfsiteCheck[
    (dfsiteCheck['e-diff'] > 100) | (dfsiteCheck['n-diff'] > 100)]
dfRivLakeSiteDets = dfRivLakeSiteDets.drop(
    dfRivLakeSiteDets[
        dfRivLakeSiteDets['site_id'].isin(site_bad.index.values)].index)
# Assumes there are no additional bad sites in the samples summary, though we
# will only be using this data for sites in dfRivLakeSiteDets anyway
dfRivLakeSiteSamps = dfRivLakeSiteSamps.drop(
    dfRivLakeSiteSamps[
        dfRivLakeSiteSamps['site_id'].isin(site_bad.index.values)].index)


# Join sample number info to main dataframe:
dfRivLakeSiteDets = dfRivLakeSiteDets.merge(dfRivLakeSiteSamps,
                                            how='left',
                                            on=["site_id", "site_name",
                                                "easting", "northing",
                                                "material"])

# Need to collapse sites with multiple E+Ns onto a single E+N, e.g. using mean
# Need to recalculate total and mean measurements when we do this
dfRivLakeSiteDets['total_measurements'] = dfRivLakeSiteDets[
    'mean_measurements'] * dfRivLakeSiteDets['num_measurements']
gtemp = dfRivLakeSiteDets.groupby(
    ['site_id', 'site_name', 'region', 'material', 'det_id', 'det_label',
     'det_desc'])
dfsiteCheck=gtemp.agg({
    'easting': 'mean',
    'northing': 'mean',
    'min_year': 'min',
    'max_year': 'max',
    'num_measurements': 'sum',
    'total_measurements': 'sum'
})
dfRivLakeSiteDets = dfRivLakeSiteDets.reset_index()
dfRivLakeSiteDets = dfRivLakeSiteDets.set_index('site_id')
dfRivLakeSiteDets['easting'] =  dfRivLakeSiteDets['easting'].astype(int)
dfRivLakeSiteDets['northing'] =  dfRivLakeSiteDets['northing'].astype(int)
dfRivLakeSiteDets['mean_measurements'] = \
    dfRivLakeSiteDets['total_measurements'] / dfRivLakeSiteDets['num_measurements']
dfRivLakeSiteDets = dfRivLakeSiteDets.drop(['index', 'total_measurements'],
                                           axis = 1)

print(str(len(site_good)) + " river and lake sites")
print(str(len(site_bad)) + " river and lake sites rejected")

# Now do this for GW sites
gtemp=dfAllGWSiteDets.groupby(['site_id', 'site_name', 'easting', 'northing',
                               'material', 'det_id', 'det_label', 'det_desc'])
dfGWSiteDets=gtemp.agg ({
    'year': ['min', 'max'],
    'num_measurements': 'sum',
    'total_measurements':  'sum'
})
dfGWSiteDets = dfGWSiteDets.reset_index()
dfGWSiteDets['mean_measurement'] = \
    dfGWSiteDets['total_measurements'] / dfGWSiteDets['num_measurements']
dfGWSiteDets=dfGWSiteDets.drop('total_measurements', axis=1)
dfGWSiteDets['region'] = dfGWSiteDets.site_id.str[0:2]
dfGWSiteDets.columns = cols2

# Remove duplicate GW sites with E+N that are different
gtemp=dfGWSiteDets.groupby('site_id')
dfsiteCheck=gtemp.agg({
    'num_measurements':'sum',
    'easting': ['max', 'min'],
    'northing': ['max', 'min']
})
dfsiteCheck.columns = ['meas', 'e-max', 'e-min', 'n-max', 'n-min']
dfsiteCheck['e-diff'] = dfsiteCheck['e-max'] - dfsiteCheck['e-min']
dfsiteCheck['n-diff'] = dfsiteCheck['n-max'] - dfsiteCheck['n-min']
site_good = dfsiteCheck[(dfsiteCheck['e-diff'] < 100) &
                        (dfsiteCheck['n-diff'] < 100)]
site_bad = dfsiteCheck[(dfsiteCheck['e-diff'] > 100) |
                       (dfsiteCheck['n-diff'] > 100)]
dfGWSiteDets = dfGWSiteDets.drop(
    dfGWSiteDets[dfGWSiteDets['site_id'].isin(site_bad.index.values)].index)

print("Finished")

print(percentiles)
# Write out file of all site and determinands
# This is a summary of data by site, determinand and year, with number of
# values and mean value for each line this is used to make the individual
# shapefiles for each HA for each determinand
dfAllRivLakeSiteDets.to_csv(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'river-lake-site-det-all-2000-2021.csv')

# This is a summary of data by site and determinand, with the number of values
# and mean value and number of samples
dfRivLakeSiteDets.to_csv(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'river-lake-site-det-summary-2000-2021.csv')

# These are the equivalents for GW data (though as noted in comments above,
# these aren't quite so carefully processed)
dfAllGWSiteDets.to_csv(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'gw-site-det-all-2000-2021.csv')
dfGWSiteDets.to_csv(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'gw-site-det-summary-2000-2021.csv')

# If we need to run the code from this point without having to read in all the
# data files again, we can read the aggregated files in
dfRivLakeSiteDets = pd.read_csv(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'river-lake-site-det-summary-2000-2021.csv')

# Fetch determinands from EIP site to make sure list matches what we are
# displaying.
determinands_url = '%sselectDeterminands.json' % paths.EIP_API_URL
det_req = json.loads(requests.get(determinands_url).text)

determinands = list(map(lambda x: int(x['id']), det_req['determinands']))
print(determinands)

# Get percentiles for each determinand from the mean_measurement of
# dfRivLakeSiteDets
detGroups = dfRivLakeSiteDets[['det_id', 'mean_measurements']]
detGroups = detGroups[detGroups['det_id'].isin(determinands)]
quantiles = np.linspace(0,1,21)
detGroups = detGroups.groupby('det_id').quantile(quantiles)
detGroups.to_csv(paths.EA_WQ_WIMS_OUTPUT_DIR + 'determinand_percentiles.csv')
# dfRivLakeSiteDets is data frame with all river sites and all determinands,
# with start and end dates, number and mean of measurements.

cols3 = ['site_id', 'site_name', 'easting', 'northing', 'num_samples',
         'num_measurements', 'min_year', 'max_year', 'region', 'HA_NUM',
         'geometry']
cols4 = ['site_id', 'site_name', 'lon', 'lat', 'num_samples',
         'num_measurements', 'mean_measurements', 'min_year', 'max_year',
         'region', 'HA_NUM', 'geometry']

# Group by site id and make geodataframe of sites
gtemp = dfRivLakeSiteDets.groupby(['region', 'site_id', 'site_name', 'easting',
                                   'northing', 'material', 'num_samples'])
dfSiteSum = gtemp.agg({
    'min_year': 'min',
    'max_year': 'max',
    'det_id': 'count',
    'num_measurements': 'sum'
})
dfSiteSum = dfSiteSum.reset_index()

print(dfSiteSum.head())
# Write to csv file
dfSiteSum.to_csv(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'river-lake-site-summary-2000-2021.csv')

# Convert to a GeoDataFrame in Easting and Northing
dfSiteSum["geometry"] = dfSiteSum.apply(
    lambda row: Point(row["easting"], row["northing"]), axis=1)
gdfSiteSumEN = geopandas.GeoDataFrame(dfSiteSum, geometry="geometry")
# Join to all WFD types
# Join to ihu sections, areas and groups and wfd catchments

# Only need to use ihu_sections as it contains ha_num and g_id
ihu_sections = geopandas.read_file(
    paths.EA_WQ_IHU_RAW_DIR + 'ihu_sections.shp')

# Only need water bodies and operational catchments as they contain the IDs of
# the basins and management catchments
wfd_water_body_catchments = geopandas.read_file(
    paths.EA_WQ_WFD_RAW_DIR +
    "EA_WFDRiverWaterBodyCatchmentsCycle2_SHP_Full/WFD_River_Water_Body_Catchments_Cycle_2.shp")
wfd_water_body_catchments = wfd_water_body_catchments.drop(['rbd_id'], axis=1)
wfd_operational_catchments = geopandas.read_file(
    paths.EA_WQ_WFD_RAW_DIR +
    "EA_WFDOperationalCatchmentsCycle2_SHP_Full/data/WFD_Surface_Water_Operational_Catchments_Cycle_2.shp")
gdfSiteSumEN.crs = ihu_sections.crs

pd.set_option('display.max_columns', None)


print(ihu_sections.crs)
print(gdfSiteSumEN.crs)
print(wfd_water_body_catchments.columns)
print(wfd_operational_catchments.crs)
print(len(gdfSiteSumEN))
# ihu_sections
# sjoin gdfSiteSumEN to IHU_groups and wfd catchments
all_catchments = gdfSiteSumEN.reset_index()

all_catchments = geopandas.tools.sjoin(all_catchments, ihu_sections,
                                       how="left")
all_catchments = all_catchments.drop(['index_right'], axis=1)

all_catchments = geopandas.tools.sjoin(all_catchments,
                                       wfd_water_body_catchments,
                                       how="left")
all_catchments = all_catchments.drop(['index_right'], axis=1)

all_catchments = geopandas.tools.sjoin(all_catchments,
                                       wfd_operational_catchments,
                                       how="left")
all_catchments = all_catchments.drop(['index_right'], axis=1)

# Get rid of those outside the coastline
all_catchments = all_catchments.dropna(subset=['HA_NUM'])
all_catchments['HA_NUM'] = all_catchments['HA_NUM'].astype(int)

# Save important columns as shapefile
all_catchments[cols3].to_file(
    paths.EA_WQ_WIMS_OUTPUT_DIR + 'river-lake-site-summary-2000-2021.shp')
print(all_catchments.columns)
print(dfRivLakeSiteDets.columns)

# Add catchment IDs to original data frame
site_catchments = all_catchments[['site_id', 'S_ID', 'HA_NUM', 'G_ID',
                                  'rbd_id', 'wb_id', 'mncat_id', 'opcat_id']]
dfRivLakeSiteDets = dfRivLakeSiteDets.merge(site_catchments, on="site_id")

# Add lat/long geometry to original data frame
transformer = Transformer.from_crs("epsg:27700", "epsg:4326", always_xy=True)
lon, lat = transformer.transform(dfRivLakeSiteDets['easting'].values,
                                 dfRivLakeSiteDets['northing'].values)

dfRivLakeSiteDets['lon']=np.around(lon,6)
dfRivLakeSiteDets['lat']=np.around(lat,6)
dfRivLakeSiteDets['coords'] = list(zip(dfRivLakeSiteDets['lon'],
                                       dfRivLakeSiteDets['lat']))
dfRivLakeSiteDets['coords'] = dfRivLakeSiteDets['coords'].apply(Point)
gdfRivLakeSiteDets = geopandas.GeoDataFrame(dfRivLakeSiteDets,
                                            geometry="coords",
                                            crs='EPSG:4326')

# Now make a lat-long geodataframe of the site information (including material)
# not sure it's possible to groupby in a geodataframe in which case we could
# use gdfRivLakeSiteDets...
print(dfRivLakeSiteDets.columns)

gtemp = dfRivLakeSiteDets.groupby(['region', 'site_id', 'site_name', 'HA_NUM',
                                   'easting', 'northing', 'material',
                                   'num_samples', 'S_ID', 'G_ID', 'rbd_id',
                                   'wb_id', 'mncat_id', 'opcat_id'])
dfSiteSum = gtemp.agg ({
    'min_year': 'min',
    'max_year': 'max',
    'det_id': 'count',
    'num_measurements': 'sum'
})
dfSiteSum = dfSiteSum.reset_index()

lon, lat = transformer.transform(dfSiteSum['easting'].values,
                                 dfSiteSum['northing'].values)

dfSiteSum['lon'] = np.around(lon, 6)
dfSiteSum['lat'] = np.around(lat, 6)
dfSiteSum['coords'] = list(zip(dfSiteSum['lon'],
                               dfSiteSum['lat']))
dfSiteSum['coords'] = dfSiteSum['coords'].apply(Point)

gdfSiteSum = geopandas.GeoDataFrame(dfSiteSum, geometry="coords")

print(len(gdfSiteSumEN))

# Now have key datasets of:
print('ihu_areas - IHU Areas')
print('')
print(ihu_areas.columns.values)
print('')
print('')

print('gdfSiteSum - Geopandas dataframe with summary site information')
print('')
print(gdfSiteSum.columns.values)
print('')
print('')

print('gdfSiteSumEN - Geopandas dataframe with summary site information, in '
      'BNG')
print('')
print(gdfSiteSumEN.columns.values)
print('')
print('')

print('dfAllRivLakeSiteDets - Full dataframe with all determinand info at '
      'each site by year - should no longer be needed')
print('')
print(dfAllRivLakeSiteDets.columns.values)
print('')
print('')

print('gdfRivLakeSiteDets - Geopandas dataframe with detailed '
      'site+determinand information')
print('')
print(gdfRivLakeSiteDets.columns.values)
print('')
print('')

# This makes all the geojson files for all chosen determinands and HAs
ihu_areas = geopandas.read_file(
    paths.EA_WQ_IHU_RAW_DIR + 'ihu_ihu_areas_wc.shp')

# Loop through IHU areas
for ha in ihu_areas['HA_NUM'].unique():
    # Subset determinand and site data frame for this area
    # Get a geodataframe of just the site info for this HA by subsetting
    # gdfSiteSum_IHU and removing columns
    gdfHASite = gdfSiteSum[gdfSiteSum['HA_NUM'] == ha]
    gdfHASite.crs = gdfRivLakeSiteDets.crs
    gdfHASiteDets = gdfRivLakeSiteDets[gdfRivLakeSiteDets['HA_NUM'] == ha]

     # Export site level info as geojson
    if (len(gdfHASite) > 0):
        gdfHARivs = gdfHASite[
            gdfHASite['material'] == 'RIVER / RUNNING SURFACE WATER']
        if (len(gdfHARivs) > 0):
            gdfHARivs.to_file(
                paths.EA_WQ_WIMS_OUTPUT_DIR + 'ha_files/riv_sites_' + str(ha) + '.geojson',
                driver="GeoJSON")

        gdfHALakes = gdfHASite[
            gdfHASite['material'] == 'POND / LAKE / RESERVOIR WATER']
        if (len(gdfHALakes) > 0):
            gdfHALakes.to_file(
                paths.EA_WQ_WIMS_OUTPUT_DIR + 'ha_files/lake_sites_' + str(ha) + '.geojson',
                driver="GeoJSON")

    # Loop through determinands
    for det in determinands:
        gdfHASiteDetX = gdfHASiteDets[gdfHASiteDets['det_id'] == det]

        # Export determinand-site info as geojson
        if (len(gdfHASiteDets) > 0):
            gdfHARivDets = gdfHASiteDetX[
                gdfHASiteDetX['material'] == 'RIVER / RUNNING SURFACE WATER']

            if (len(gdfHARivDets) > 0):
                gdfHARivDets.to_file(
                    paths.EA_WQ_WIMS_OUTPUT_DIR + 'determinands/riv_sites_' + 'det_' + str(ha) + '_' + str(det) + '.geojson',
                    driver="GeoJSON")

            gdfHALakeDets = gdfHASiteDetX[
                gdfHASiteDetX['material'] == 'POND / LAKE / RESERVOIR WATER']

            if (len(gdfHALakeDets) > 0):
                gdfHALakeDets.to_file(
                    paths.EA_WQ_WIMS_OUTPUT_DIR + 'determinands/lake_sites_' + 'det_' + str(ha) + '_' + str(det) + '.geojson',
                    driver="GeoJSON")
