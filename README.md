# NCEA Freshwater Workflow
This module contains two features:
-	Functions to return data availability information from a selection of environmental networks
-	Functions to return catchment data for any coordinates

_Note, no data files are included in this repository_

![Workflow schematic](https://github.com/NERC-CEH/NCEA-Freshwater-Workflow/blob/master/workflow_schematic.png)

## Network data availability
All functions are contained in **network_data_availability.py**
These create site and data availability metadata files for the following networks:

- EA water quality (EA_WQ)
- EA Macroinvertebrates (EA_INV)
- EA Macrophyte (EA_MACP)
- EA Diatom (EA_DIAT)
- EA fish count data (EA_FISH)
- Riverfly survey (RS)
- FreshwaterWatch (FWW)
- National river flow archive (NRFA)

![Workflow schematic](https://github.com/NERC-CEH/NCEA-Freshwater-Workflow/blob/master/backend_schematic.png)

Each network has a “**create_{NETWORK_ID}_metadata()**” function which goes to
the data source and creates 3 CSV metadata files:
- site_register_{NETWORK_ID}.csv
- data_type_register_{NETWORK_ID}.csv
- data_availability_{NETWORK_ID}.csv

Use **create_all_metadata_csv()** to run these for all networks sequentially.

_Note, Some networks read data from downloaded files others from APIs:_
- _File_: EA_INV, EA_MACP, EA_DIAT, EA_FISH, RS, FWW
- _API_: EA_WQ
- _Combination of both_: NRFA

#### site_register_{NETWORK_ID}.csv
This lists the available sites for the network with the following metadata:
- SITE_ID
- SITE_NAME
- NETWORK_ID
- LATITUDE
- LONGITUDE

#### data_type_register_{NETWORK_ID}.csv
This lists the data types available from the network with the following metadata:

- DTYPE_ID
- DTYPE_NAME
- NETWORK_ID
- DTYPE_DESC
- UNITS
- MEAN_MIN
- MEAN_PERCENTILE_20
- MEAN_PERCENTILE_40
- MEAN_PERCENTILE_60
- MEAN_PERCENTILE_80
- MEAN_MAX
- MEAN_MEAN
- MEAN_COUNT

_Note, the "MEAN" data is calculated from site means from each site. For example,
we have 100 sites measuring nitrate, we take all the mean values from each site
(so 100 values) and calculate this stats with those (hence the MEAN_MEAN)._

#### data_availability_{NETWORK_ID}.csv
This lists summary information on data availability for each site and data types
from the network. With the following metadata:

- SITE_ID
- NETWORK_ID
- DTYPE_ID
- START_DATE
- END_DATE
- SITE_VALUE_COUNT
- SITE_VALUE_MEAN

### JSON files
The data in these CSV can then be converted to JSON. There are two JSON files
that can be created.

#### network_info.json
This contains overall information on the network plus site wide data for each
data type.
It is created by the function **make_network_dict()**.

It has the following structure (using example):

    [{
      "network_id": "EA_WQ",
      "network_name": "EA Water Quality",
      "network_desc": "EA Water quality monitoring network",
      "folder": "EA_water_quality",
      "shape": "square",
      "access": "geojson",
      "updates": "Realtime data available",
      "website": "https://environment.data.gov.uk/water-quality/view/landing",
      "dtype_ids": [{
          "dtype_id": "0117",
          "dtype_name": "Nitrate-N",
          "dtype_desc": "Nitrate as N",
          "network_id": "EA_WQ",
          "mean_min": 0,
          "percentile_20": 1.62,
          "percentile_40": 4.01,
          "percentile_60": 6.36,
          "percentile_80": 8.65,
          "mean_max": 370.08,
          "mean_mean": 5.78,
          "mean_count": 4060
      }]
    }]


#### {AREA_ID}_{NETWORK_ID}_availability.json
This combines data from all three CSVs in one JSON. To avoid large file sizes
they are saved separately by area.
It is created by the function **availability_geojson_split()**.

By default this function will create JSON for all networks (provided the CSVs have
been created), but you can specify particular networks with the **networks** argument. It is also possible to specify the areas the files are split by. By default this is by operational catchment
area, but IHU area and IHU group can also be specified using the **split_area** argument.

It has the following structure (using example):

    {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        },
        "features": [{
            "type": "Feature",
            "properties": {
                "site_id": "69032",
                "site_name": "Alt at Kirkby",
                "network_id": "NRFA",
                "opcat_id": "3008",
                "dtypes": [{
                    "dtype_id": "gdf",
                    "dtype_name": "Gauged daily flows",
                    "dtype_desc": null,
                    "start_date": "1977-10-01T00:00:00Z",
                    "end_date": "2020-09-30T00:00:00Z",
                    "value_count": 15492.0,
                    "value_mean": 1.442
                }],
                "dtype_count": 1
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-2.96611, 53.5052]
            }
        }]
    }

## Catchment data
A selection of functions are available in **catchment_tools.py** that allow the
extraction of catchment data for any grid point. There are two types of data; FEH descriptors and LCM data, i.e. ratios of the different land cover types in the catchment.

The main functionality is available through the CatchmentData class. See example below to demonstrate its use and capabilities.


    import catchment_tools as c_tools

    # Set coordinates using easting and northing
    easting = 80100
    northing = 480500

    # Setting the snap_to_river arg will automatically shift the given coordinates
    # to the nearest river. This is handy where coordinates are not precise.
    catch = c_tools.CatchmentData(easting, northing, snap_to_river=True)

    # Get FEH data
    FEH_data = catch.get_FEH_data()

    # Get LCM data (must specify the LCM version by year; 2000, 2007, 2015
    # available)
    FEH_data = catch.get_LCM_data(2015)

    # Alternatively, request multiple data, joined together using the get_data
    # method. By default all, are fetched, use the following to specify:
    # "FEH", "LCM2000", "LCM2007", "LCM2015"
    data = catch.get_data(desc_types="all")


In the file **process_catchment_data.py** there is the function **network_catchment_data()** that demonstrates combining the two major parts by reading in the coordinates from the network files and extracts catchment data.


## paths.py
All data file paths are set in **paths.py** in an OS agnostic way.
