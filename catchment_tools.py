# -*- coding: utf-8 -*-
"""
Tools for retrieving catchment data.

"""
import paths

import os
import pandas as pd

import geopandas as gpd
import numpy as np
import numpy.ma as ma
from math import sqrt

from shapely.geometry import Point
from shapely.geometry import box
from shapely.ops import nearest_points

import rasterio
from rasterio.windows import Window
from rasterio.plot import show
from rasterio.plot import show_hist
from rasterio.mask import mask

from fiona.crs import from_epsg

import json

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm


# LCM2015 dataset is split into layers, named at these coordinates.
LCM2015_EASTING_LAYERS = (35000, 135000, 235000, 335000, 435000, 535000,
                          635000, 735000)
LCM2015_NORTHING_LAYERS = (50000, 150000, 250000, 350000, 450000, 550000,
                           650000, 750000, 850000, 950000, 1050000, 1150000,
                           1250000)

# Map for how many extra depths to check when finding closest grids. (See
# closest_CCAR_above_val for more details).
DEPTH_CHECK_MAP = {
    0: 0,
    1: 0,
    2: 0,
    3: 1,
    4: 1,
    5: 2,
    6: 2,
    7: 2,
    8: 3,
    9: 3,
    10: 4,
    11: 4,
    12: 4,
    13: 5,
    14: 5,
    15: 5,
    16: 4,
    17: 3,
    18: 2,
    19: 1,
    20: 0,
}


class CatchmentData(object):
    _descriptor_type_dirs = {
        "FEH_gb": paths.GB_FEH_DIR,
        "FEH_ni": paths.NI_FEH_DIR,
        "LCM_2000_gb": paths.GB_LCM_2000_DIR,
        "LCM_2000_ni": paths.NI_LCM_2000_DIR,
        "LCM_2007_gb": paths.GB_LCM_2007_DIR,
        "LCM_2007_ni": paths.NI_LCM_2007_DIR,
        "LCM_2015_gb": paths.GB_LCM_2015_DIR,
        "LCM_2015_ni": paths.NI_LCM_2015_DIR,
    }

    # Aggregates of LCM classes used for NRFA. I.e. LCM and many classes but
    # NRFA groups them into a smaller subset.
    _LCM_class_aggregates = {
        "Woodland": [
            "2000_11",
            "2000_21",
            "2007_1",
            "2007_2",
            "2015_1",
            "2015_2",
        ],
        "Arable and Horticulture": [
            "2000_41",
            "2000_42",
            "2000_43",
            "2007_3",
            "2015_3",
        ],
        "Grassland": [
            "2000_51",
            "2000_52",
            "2000_61",
            "2000_71",
            "2000_81",
            "2000_91",
            "2000_111",
            "2007_4",
            "2007_5",
            "2007_6",
            "2007_7",
            "2007_8",
            "2007_9",
            "2015_4",
            "2015_5",
            "2015_6",
            "2015_7",
            "2015_8",
        ],
        "Heath/Bog": [
            "2000_101",
            "2000_102",
            "2000_121",
            "2000_151",
            "2007_10",
            "2007_11",
            "2007_12",
            "2007_13",
            "2015_9",
            "2015_10",
            "2015_11",
        ],
        "Bareground": [
            "2000_161",
        ],
        "Inland Rock": [
            "2007_14",
            "2015_12",
        ],
        "Water": [
            "2000_121",
            "2000_131",
            "2007_15",
            "2007_16",
            "2015_13",
            "2015_14",
        ],
        "Coastal": [
            "2000_181",
            "2000_191",
            "2000_201",
            "2000_211",
            "2000_212",
            "2007_17",
            "2007_18",
            "2007_19",
            "2007_20",
            "2007_21",
            "2015_15",
            "2015_16",
            "2015_17",
            "2015_18",
            "2015_19",
        ],
        "Urban": [
            "2000_171",
            "2000_172",
            "2007_22",
            "2007_23",
            "2015_20",
            "2015_21",
        ],
        "Unknown": [
            "2000_9999",
            "2007_9999",
            "2015_9999",
        ],
    }

    # The full names of the FEH codes
    FEH_code_map = {
        "CCAR": "ihdtm-catchment-area",
        "HGHT": "ihdtm-height",
        "Q__C": "ddf-c-catchment",
        "Q__E": "ddf-e-catchment",
        "Q__F": "ddf-f-catchment",
        "Q_D1": "ddf-d1-catchment",
        "Q_D2": "ddf-d2-catchment",
        "Q_D3": "ddf-d3-catchment",
        "QALT": "altbar",
        "QASB": "aspbar",
        "QASV": "aspvar",
        "QB19": "BFIHOST19",
        "QBFI": "bfihost",
        "QDPB": "dplbar",
        "QDPS": "dpsbar",
        "QFAR": "farl",
        "QFPD": "mean-flood-plain-depth",
        "QFPL": "mean-flood-plain-location",
        "QFPX": "mean-flood-plain-extent",
        "QLDP": "ldp",
        "QPRW": "propwet",
        "QR1D": "rmed-1d",
        "QR1H": "rmed-1h",
        "QR2D": "rmed-2d",
        "QS47": "saar-1941-1970",
        "QS69": "saar-1961-1990",
        "QSPR": "sprhost",
        "QUC2": "urbconc-2000",
        "QUCO": "urbconc-1990",
        "QUE2": "urbext-2000",
        "QUEX": "urbext-1990",
        "QUL2": "urbloc-2000",
        "QULO": "urbloc-1990",
        "RM_C": "ddf-c",
        "RM_E": "ddf-e",
        "RM_F": "ddf-f",
        "RMD1": "ddf-d1",
        "RMD2": "ddf-d2",
        "RMD3": "ddf-d3",
    }

    def __init__(self, easting, northing, station="", snap_to_river=False):
        self.easting_raw = easting
        self.northing_raw = northing

        if snap_to_river:
            self.easting, self.northing = self._river_snapping()
        else:
            self.easting = self.easting_raw
            self.northing = self.northing_raw

        self.region = self._get_region()

        self.station = station

        # Set up catchment descriptor attributes
        self.FEH_data = None
        self.LCM_2000_data = None
        self.LCM_2007_data = None
        self.LCM_2015_data = None
        self.QCN_data = None

        self._valid_desc_types = ["FEH", "LCM2000", "LCM2007", "LCM2015"]
        self._valid_LCM_years = [2000, 2007, 2015]

    def _river_snapping(self):
        cell_dict = closest_ccar_above_val(self.easting_raw,
                                           self.northing_raw,
                                           min_ccar=200,
                                           max_depths=20)
        if cell_dict is None:
            raise UserWarning("No cell found close enough for river snapping")

        print("Coords snapped to river. From (%s, %s) to (%s, %s)" % (
            self.easting_raw, self.northing_raw, cell_dict["easting"],
            cell_dict["northing"]
        ))

        return cell_dict["easting"], cell_dict["northing"]

    def _get_region(self):
        """
        Work out if coordinates are for Great Britian (GB) or Northern Ireland
        (NI). Do this by checking if the fall within one of the two bounding
        boxes that cover NI. If not, assume GB.

        Bounding boxes are defined as [x_min,y_min,x_max,y_max]

        """
        # West and east bounding boxes for NI
        west_ni = [000000, 469190, 143723, 614827]
        east_ni = [143723, 469190, 185797, 597050]

        region = "gb"

        # Test west bounding box
        if self.easting >= west_ni[0] and self.easting <= west_ni[2]:
            if self.northing >= west_ni[1] and self.northing <= west_ni[3]:
                region = "ni"

        # Test east bounding box
        elif self.easting >= east_ni[0] and self.easting <= east_ni[2]:
            if self.northing >= east_ni[1] and self.northing <= east_ni[3]:
                region = "ni"

        return region

    def _read_directory_tifs(self, directory, desc_type):
        """
        Read the tif files in the given directory. Each tif contains data for
        a particluar catchment descriptor. For each, establish the descriptor
        type (PROPERTY_ITEM) and extract the value for it at the class
        coordinates.
        Append all the values (and their type) to a dataframe.

        """
        # Create a pandas dataframe to hold the data.
        df = pd.DataFrame(columns=['PROPERTY_ITEM', 'PROPERTY_VALUE'])
        for dir_path, dir_name_list, file_name_list in os.walk(directory):
            for filename in file_name_list:
                # If this is not a tif file.
                if not filename.endswith('.tif'):
                    # Skip it
                    continue

                # Open the tif file.
                file_path = os.path.join(dir_path, filename)
                raster_file = rasterio.open(file_path)

                if desc_type == "FEH":
                    # Get the descriptor code (from filename).
                    descriptor_code = str(filename[-11:-7])
                elif desc_type == "LCM":
                    lcm_year = str(filename.split("_")[1])
                    lcm_code = str(filename.split("_")[2])[:-4]
                    descriptor_code = lcm_year + "_" + lcm_code

                # Use the class coordinates and get the value of raster.
                val = list(raster_file.sample([
                    (self.easting, self.northing)]))[0][0]

                # Create a list to use to append the data to the dataframe.
                values_list = [descriptor_code, val]
                # Make a series to make a row.
                value_series = pd.Series(values_list, index=df.columns)
                df = df.append(value_series, ignore_index=True)
                raster_file.close()

        # Make sure values are floats
        df["PROPERTY_VALUE"] = df["PROPERTY_VALUE"].astype(float)

        return df

    def _add_db_columns(self, desc_df, group="", method="", comment="",
                        title="", units="", source="", remove_errors=True):
        """
        Restructure dataframe to have columns required for DB table.

        """
        if remove_errors:
            desc_df = desc_df[desc_df["PROPERTY_VALUE"] > -5000]

        desc_df["STATION"] = self.station
        desc_df["PROPERTY_GROUP"] = group
        desc_df["PROPERTY_METHOD"] = method
        desc_df["PROPERTY_COMMENT"] = comment
        desc_df["TITLE"] = title
        desc_df["UNITS"] = units
        desc_df["SOURCE_VALUE"] = source

        # Arrange column order.
        return desc_df[["STATION", "PROPERTY_GROUP", "PROPERTY_ITEM",
                        "PROPERTY_VALUE", "PROPERTY_METHOD",
                        "PROPERTY_COMMENT", "TITLE", "UNITS", "SOURCE_VALUE"]]

    def _aggregate_LCM_classes(self, LCM_data, year):
        """
        LCM data has a set of classes, e.g. Improved grassland and Neutral
        grassland, with assciated codes (which can vary between years). For
        this work we define a simplfied set of classes which aggregates LCM
        classes, e.g. Grassland, which includes both 'Improved' and 'Neutral'.

        The mapping between the simplied classes and the LCM class codes is
        given in the dictionary self._LCM_class_aggregates.

        """
        group = "lcm%snrfav2021" % year

        for agg_class, lcm_classes in self._LCM_class_aggregates.items():
            # Extract details from the LCM classes within the aggregate class
            valid_classes = LCM_data[
                LCM_data["PROPERTY_ITEM"].isin(lcm_classes)]

            if len(valid_classes) == 0:
                continue

            # LCM classes have format 'year_class', extract just the class
            # numbers so we can create a sting for teh SOURCE_VALUE column
            valid_cls_nums = [cls.split("_")[-1]
                              for cls in valid_classes["PROPERTY_ITEM"]]
            source = "+".join(valid_cls_nums)

            # Sum the total percentage across aggregate classes
            agg_total = valid_classes["PROPERTY_VALUE"].sum()

            new_row = {
                "STATION": self.station,
                "PROPERTY_GROUP": group,
                "PROPERTY_ITEM": agg_class,
                "PROPERTY_VALUE": agg_total,
                "PROPERTY_METHOD": "automatic",
                "PROPERTY_COMMENT": "",
                "TITLE": agg_class,
                "UNITS": "proportion",
                "SOURCE_VALUE": source
            }

            LCM_data = LCM_data.append(new_row, ignore_index=True)

        return LCM_data

    def get_FEH_data(self, convert_codes=False):
        """
        Extract FEH data for catchment. Data for each grid are saved in a tif
        files.

        Arg:
            convert_codes: Bool
                Optionally convert FEH codes to their full names.

        """
        # Get appropriate tif directory.
        desc_dir_key = "FEH_%s" % self.region
        desc_dir = self._descriptor_type_dirs.get(desc_dir_key)
        if desc_dir is None:
            raise UserWarning("region attribute: '%s', is not valid"
                              % self.region)

        if self.FEH_data is None:
            FEH_data = self._read_directory_tifs(desc_dir, "FEH")

            if convert_codes is True:
                FEH_data = FEH_data.replace(self.FEH_code_map)

            self.FEH_data = self._add_db_columns(FEH_data,
                                                 group="FEH",
                                                 method="automatic")

        return self.FEH_data

    def get_LCM_data(self, year=2015):
        """
        Extract land cover map (LCM) data for catchment, specifying the LCM
        version by its year. Data for each grid are saved in a tif files.

        """
        # Get appropriate tif directory.
        desc_dir_key = "LCM_%s_%s" % (year, self.region)
        desc_dir = self._descriptor_type_dirs.get(desc_dir_key)
        if desc_dir is None:
            if year not in self._valid_LCM_years:
                raise UserWarning("Invalid year: %s. Valid years are: %s" % (
                                      self.region,
                                      ", ".join(self._valid_LCM_years)
                                  ))
            else:
                raise UserWarning("region attribute: '%s', is not valid"
                                  % self.region)

        # Establish the LCM attribute by year
        if year == 2000:
            LCM_data = self.LCM_2000_data
            attr = "LCM_2000_data"
        elif year == 2007:
            LCM_data = self.LCM_2007_data
            attr = "LCM_2007_data"
        elif year == 2015:
            LCM_data = self.LCM_2015_data
            attr = "LCM_2015_data"
        else:
            raise UserWarning("Invalid year: %s. Valid years are: %s" % (
                                  self.region,
                                  ", ".join(self._valid_LCM_years)
                              ))

        if LCM_data is None:
            LCM_data = self._read_directory_tifs(desc_dir, "LCM")

            group = "lcm%sv2021" % year
            source = LCM_data["PROPERTY_ITEM"]
            LCM_data = self._add_db_columns(LCM_data,
                                            group=group,
                                            method="automatic",
                                            units="proportion",
                                            source=source)

            # Divide by 400 to get the area in square km.
            LCM_data["PROPERTY_VALUE"] = LCM_data["PROPERTY_VALUE"] / 400.
            # Get the sum of area.
            catchment_area = LCM_data["PROPERTY_VALUE"].sum()
            # Calculate the percentage of each category.
            LCM_data["PROPERTY_VALUE"] = (
                LCM_data["PROPERTY_VALUE"] / catchment_area) * 100.

            # Join LCM classes into simplified classes
            LCM_data = self._aggregate_LCM_classes(LCM_data, year)

            setattr(self, attr, LCM_data)

        return LCM_data

    def get_data(self, desc_types="all", savepath=None):
        """
        Extract catchment data and convert to format for database.

        """
        if isinstance(desc_types, str):
            desc_types = [desc_types]
        if desc_types[0] == "all":
            desc_types = self._valid_desc_types
        else:
            for desc_type in desc_types:
                if desc_type not in self._valid_desc_types:
                    raise UserWarning("Invalid descriptor type: %s. Valid "
                                      "descriptor types are: %s" % (
                                          desc_type,
                                          ", ".join(self._valid_desc_types)
                                      ))

        # Fetch given descriptor types, convert to DB format and combine.
        descs_df = None
        for desc_type in desc_types:
            if desc_type == "FEH":
                self.get_FEH_data()
                if descs_df is None:
                    descs_df = self.FEH_data
                else:
                    descs_df = descs_df.append(self.FEH_data,
                                               ignore_index=True)

            elif desc_type == "LCM2000":
                self.get_LCM_data(2000)
                if descs_df is None:
                    descs_df = self.LCM_2000_data
                else:
                    descs_df = descs_df.append(self.LCM_2000_data,
                                               ignore_index=True)

            elif desc_type == "LCM2007":
                self.get_LCM_data(2007)
                if descs_df is None:
                    descs_df = self.LCM_2007_data
                else:
                    descs_df = descs_df.append(self.LCM_2007_data,
                                               ignore_index=True)

            elif desc_type == "LCM2015":
                self.get_LCM_data(2015)
                if descs_df is None:
                    descs_df = self.LCM_2015_data
                else:
                    descs_df = descs_df.append(self.LCM_2015_data,
                                               ignore_index=True)

        if savepath is not None:
            descs_df.to_csv(savepath)

        return descs_df


def get_QCN_data(stations):
    """
    Create QCN (Polygon centroids) Dataset.
    Centroids csv file is a direct export from the centroid calculation in
    ArcGIS.
    For every new catchment you have to re-export this file after adding
    the new catchment.
    (If it's only one station it may be quicker to copy and paste the
    coordinated directly from ArcGIS).

    """
    qcn_data = pd.read_csv(paths.QCN_DIR + "catchments_all.csv")
    qcn_data = qcn_stations_df[qcn_data["STATION"].isin(stations)]
    if len(qcn_data) == 0:
        print("No valid QCN station IDs given")
        return

    print("Creating QCN (Polygon centroids) dataset *************************")
    qcndf = pd.DataFrame(
        columns=["STATION", "PROPERTY_ITEM", "PROPERTY_VALUE"])

    for index, row in qcn_data.iterrows():
        station_name = row["STATION"]
        QCNE = int(row["QCNE"])
        QCNN = int(row["QCNN"])
        list_qcne = [station_name, "QCNE", QCNE]
        list_qcnn = [station_name, "QCNN", QCNN]

        # Make a series per row
        value_series_qcne = pd.Series(list_qcne, index=qcndf.columns)
        value_series_qcnn = pd.Series(list_qcnn, index=qcndf.columns)

        qcndf = qcndf.append(value_series_qcne, ignore_index=True)
        qcndf = qcndf.append(value_series_qcnn, ignore_index=True)

    # Add Columns to match NRFA Oracle table
    qcndf["PROPERTY_GROUP"] = "FEH"
    qcndf["PROPERTY_METHOD"] = "automatic"
    qcndf["PROPERTY_COMMENT"] = ""
    qcndf["TITLE"] = ""
    qcndf["UNITS"] = ""
    qcndf["SOURCE_VALUE"] = ""

    # Rearrange columns to match Oracle table
    return qcndf[["STATION", "PROPERTY_GROUP", "PROPERTY_ITEM",
                  "PROPERTY_VALUE", "PROPERTY_METHOD", "PROPERTY_COMMENT",
                  "TITLE", "UNITS", "SOURCE_VALUE"]]


def base_round(x, base):
    """
    Returns a value rounded to base number.

    """
    return base * round(x / base)


def get_layer(easting, northing):
    """
    This function returns a string based on two lists for easting and northing.
    It is used to derive the name of the LCM2015 dataset, based on the
    coordinates given by the user.

    Layer name has format: "T(easting)_(northing)"

    """
    label_easting = min(LCM2015_EASTING_LAYERS, key=lambda x:abs(x-easting))
    label_northing = min(LCM2015_NORTHING_LAYERS, key=lambda x:abs(x-northing))
    return "T" + str(label_easting) + "_" + str(label_northing)


def read_ccar(easting, northing, raster_file=None):
    """
    Get CCAR value from the dataset.
    CCAR is the number of grid cells (at 50m^2) that flow into the cell at the
    given coordinates.

    """
    if raster_file is None:
        raster_file = rasterio.open(paths.CCAR_FILE)
        close_raster = True
    else:
        # If raster file is given, do not close it in this function
        close_raster = False

    # Round easting and northing to the closest 50m
    easting = base_round(easting, 50)
    northing = base_round(northing, 50)

    # Using the rasterio.sample method to get CCAR at specific point
    val = list(raster_file.sample([(easting, northing)]))[0][0]

    if close_raster:
        raster_file.close()

    if val == 2147483647:
        # Special bad value, convert to -999
        return -999
    else:
        return val


def read_ccar_square(easting, northing, depth, border_only=False,
                     raster_file=None):
    """
    Read in the cells around the central easting and northing given, at the
    given depth.
    If border_only is True, the cells withing the border are not read.
    For example, a depth of 1 means the 8 cells the surround the central cell
    are read, as oppose to all 9. At depth 2, the 16 cells that surround the
    central 9 are read.. and so on.

    """
    cell_dicts = []

    if depth == 0:
        # Handle 0 case separately (is just the given centre cell).
        ccar = read_ccar(easting, northing, raster_file)
        cell_dict = {}
        cell_dict["easting"] = easting
        cell_dict["northing"] = northing
        cell_dict["ccar"] = ccar
        cell_dict["distance"] = 0
        cell_dicts.append(cell_dict)

    else:
        if border_only is True:
            # To pick out only the border cells, we constrain easting or
            # northing to always be at max or min depth.
            e_depth_range = [-depth, depth]
        else:
            e_depth_range = range(-depth, depth + 1)

        for e_depth in e_depth_range:
            for n_depth in range(-depth, depth + 1):
                this_easting = easting + (50 * e_depth)
                this_northing = northing + (50 * n_depth)

                ccar = read_ccar(this_easting, this_northing, raster_file)
                dist = sqrt((this_easting - easting)**2 +
                            (this_northing - northing)**2)

                cell_dict = {}
                cell_dict["easting"] = this_easting
                cell_dict["northing"] = this_northing
                cell_dict["ccar"] = ccar
                cell_dict["distance"] = dist
                cell_dicts.append(cell_dict)

        if border_only is True:
            # Do remaining on cells on top and bottom borders (leaving out the
            # corners as they are already done).
            for n_depth in [-depth, depth]:
                for e_depth in range(-depth + 1, depth):
                    this_easting = easting + (50 * e_depth)
                    this_northing = northing + (50 * n_depth)

                    ccar = read_ccar(this_easting, this_northing, raster_file)
                    dist = sqrt((this_easting - easting)**2 +
                                (this_northing - northing)**2)

                    cell_dict = {}
                    cell_dict["easting"] = this_easting
                    cell_dict["northing"] = this_northing
                    cell_dict["ccar"] = ccar
                    cell_dict["distance"] = dist
                    cell_dicts.append(cell_dict)

    return cell_dicts


def largest_ccar_close_by(easting, northing, depth=1):
    """
    Search the cells around the given cell to a given depth (e.g. 1 gives
    surrounding 9 cells, 2 is 25 cells).

    The absolute highest value is always returned, however, if > 1 max values
    are found, the closest is returned.
    If > 1 max values at the same distance are returned, the first found is
    returned, which is southern, then western, most.

    """
    # Round easting and northing to the closest 50m
    easting = base_round(easting, 50)
    northing = base_round(northing, 50)

    # Open the raster file
    raster_file = rasterio.open(paths.CCAR_FILE)

    best_cell = None

    cell_dicts = read_ccar_square(easting, northing, depth,
                                  raster_file=raster_file)

    for cell_dict in cell_dicts:
        if best_cell is None:
            best_cell = cell_dict

        elif cell_dict["ccar"] >= best_cell["ccar"]:
            # If equal maxes, check distance.
            if cell_dict["ccar"] == best_cell["ccar"] and \
                    cell_dict["distance"] >= best_cell["distance"]:
                # Is no closer so do not replace.
                continue

            best_cell = cell_dict

    raster_file.close()

    return best_cell


def closest_ccar_above_val(easting, northing, min_ccar=10, max_depths=20):
    """
    Find the cell nearest to a given cell that has at least the given CCAR
    value.

    """
    # Round easting and northing to the closest 50m
    easting = base_round(easting, 50)
    northing = base_round(northing, 50)

    # Open the raster file
    raster_file = rasterio.open(paths.CCAR_FILE)

    best_cell = None
    extra_depths = None
    for depth in range(0, max_depths + 1):
        if extra_depths is not None:
            if extra_depths == 0:
                break
            else:
                extra_depths -= 1

        # Gather cell data for growing squares around the centre.
        cell_dicts = read_ccar_square(easting, northing, depth,
                                      border_only=True,
                                      raster_file=raster_file)

        for cell_dict in cell_dicts:
            if cell_dict["ccar"] >= min_ccar:
                # We found a cell that passes the min ccar test, but there
                # could be others in the square and we want the closest.
                if best_cell is None:
                    best_cell = cell_dict

                elif cell_dict["distance"] <= best_cell["distance"]:
                    if cell_dict["distance"] == best_cell["distance"]:
                        # If equal distance, check CCAR
                        if cell_dict["ccar"] <= best_cell["ccar"]:
                            # CCAR no larger so do not replace.
                            continue

                    best_cell = cell_dict

                # Our search expands by square size. This means as the square
                # gets bigger, there is a chance that the corners of an inner
                # square and further away than the sides of the next outer
                # square.
                # This means, once the depth is a certain size, we need to keep
                # checking further depths to make sure we actually have the
                # closest. How many extra depths to check depends on the
                # current depth and has been worked out and stored in
                # DEPTH_CHECK_MAP.
                extra_depths = DEPTH_CHECK_MAP[depth]

    raster_file.close()

    if best_cell is None:
        print("No cell found with CCAR > %s in surrounding area. %s depths "
              "search" % (min_ccar, max_depths))

    return best_cell


def getFeatures(gdf):
    """
    Function to parse features from GeoDataFrame in such a manner that rasterio
    wants them.

    """
    return [json.loads(gdf.to_json())['features'][0]['geometry']]


def crop_grid(easting, northing, depth):
    """
    This function creates a clipped version of CCAR file, since CCAR file is
    large and kernel crashes (low memory).
    It creates a file called "out_tif.tif". Always overwrite previous.
    Use this to get the cropped version of CCAR below, for plotting
    input is easting, northing of centre cell, and depth: number of cells
    either side.

    """
    raster_file = rasterio.open(paths.CCAR_FILE)
    radius = depth * 50
    bbox = box(easting - radius, northing - radius,
               easting + radius, northing + radius)
    geo  = gpd.GeoDataFrame({'geometry': bbox}, index=[0],
                            crs=from_epsg(27700))
    geo = geo.to_crs(crs=raster_file.crs.data)
    coords = getFeatures(geo)
    out_img, out_transform = mask(dataset=raster_file, shapes=coords,
                                  crop=True)
    out_meta = raster_file.meta.copy()
    epsg_code = int(raster_file.crs.data['init'][5:])
    out_meta.update({"driver": "GTiff",
                     "height": out_img.shape[1],
                     "width": out_img.shape[2],
                     "transform": out_transform,
                     "crs": 27700})
    with rasterio.open(paths.TEMP_CCAR_FILE, "w", **out_meta) as dest:
        dest.write(out_img)


def plot_grid_and_values(pts):
    # Usually 3 points, colour them red, orange and blue
    cols = ['red', 'orange', 'blue']
    raster_file = rasterio.open(paths.TEMP_CCAR_FILE)
    left = raster_file.bounds[0] + 25
    top = raster_file.bounds[3] - 25
    fig, ax = plt.subplots(figsize=(20, 20))
    show(raster_file, ax=ax, cmap="Greens", norm=LogNorm())
    arr = raster_file.read(1)
    arr_masked = ma.array(arr)
    null_val = 2147483647
    arr_masked[arr == null_val] = ma.masked

    for (j, i), label in np.ndenumerate(arr_masked):
        if label != null_val:
            ax.text((left + (i * 50)), (top - (j * 50)), label, ha='center',
                    va='center')

    for i in range(len(pts)):
        plt.scatter(pts[i][0], pts[i][1], s=300, c=cols[i], marker='o')
