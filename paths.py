# -*- coding: utf-8 -*-
"""
File and directory paths.

"""
import os

def make_fpath(dirs):
    """
    Convert a list of directories into an OS path.
    Agnostic of OS path separators (i.e. '\' for windows and '/' for Unix)

    """
    return os.sep.join(dirs) + os.sep


# --- Base directory lists ----------------------------------------------------
root_dirs = [
    "%sprj" % os.sep,
    "nrfa_dev",
    "NCEA",
]

metadata_dirs = root_dirs + ["metadata"]

RAW_DATA_DIR = make_fpath(root_dirs + ["raw_data"])
OUTPUT_DIR = make_fpath(root_dirs + ["output_files"])


# --- Catchment descriptor paths ----------------------------------------------
# Directories
GB_FEH_DIR = make_fpath(root_dirs + ["raw_data", "tifs", "FEH", "GB"])
NI_FEH_DIR = make_fpath(root_dirs + ["raw_data", "tifs", "FEH", "NI"])
GB_LCM_2000_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs",
                                          "LCM", "GB","2000"])
NI_LCM_2000_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs",
                                          "LCM", "NI", "2000"])
GB_LCM_2007_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs",
                                          "LCM", "GB", "2007"])
NI_LCM_2007_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs",
                                          "LCM", "NI", "2007"])
GB_LCM_2015_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs",
                                          "LCM", "GB", "2015"])
NI_LCM_2015_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs",
                                          "LCM", "NI", "2015"])
QCN_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "csvs"])
CCAR_DIR = make_fpath(root_dirs + ["raw_data", "catchment", "tifs", "CCAR"])

# Files
CCAR_FILE = "%sCCAR.tif" % CCAR_DIR
TEMP_CCAR_FILE = "%stemp_CCAR.tif" % CCAR_DIR


# --- EA Water quality paths --------------------------------------------------
# Directories
# Raw
EA_WQ_WIMS_RAW_DIR = make_fpath(root_dirs + ["raw_data", "EA_WQ", "WIMS"])
EA_WQ_IHU_RAW_DIR = make_fpath(root_dirs + ["raw_data", "EA_WQ", "IHU"])
EA_WQ_WFD_RAW_DIR = make_fpath(root_dirs + ["raw_data", "EA_WQ", "WFD"])
# Output
EA_WQ_WIMS_OUTPUT_DIR = make_fpath(root_dirs + ["output_files", "EA_WQ",
                                                "WIMS"])

# URLs
WQ_BATCH_URL = "https://environment.data.gov.uk/water-quality/batch/measurement"
WQ_API_URL = "https://environment.data.gov.uk/water-quality/id/sampling-point"


# --- EA Bio paths ------------------------------------------------------------
# URLs
EA_ECO_BASE_URL = "https://environment.data.gov.uk/ecology/api/v1/"


# --- Metadata ----------------------------------------------------------------
METADATA_CSV_DIR = make_fpath(metadata_dirs + ["csvs"])
METADATA_GJSON_DIR = make_fpath(metadata_dirs + ["geoJSON"])
METADATA_IHU_DIR = make_fpath(metadata_dirs + ["IHU"])

DTYPE_REGISTER_FPATH = "%sdata_type_register_{NETWORK}.csv" % METADATA_CSV_DIR
SITE_REGISTER_FPATH = "%ssite_register_{NETWORK}.csv" % METADATA_CSV_DIR
DATA_AVAILABILITY_FPATH = "%sdata_availability_{NETWORK}.csv" % METADATA_CSV_DIR

# --- Other -------------------------------------------------------------------
# URLs
EIP_API_URL = "https://eip.ceh.ac.uk/hydrology-ukscape/"
