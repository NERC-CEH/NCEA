# -*- coding: utf-8 -*-
"""
File and directory paths.

"""
import os
import config

def make_fpath(dirs):
    """
    Convert a list of directories into an OS path.
    Agnostic of OS path separators (i.e. '\' for windows and '/' for Unix)

    If list item in list is not a file, determined by a "." file extension,
    then a path separator is added to the end.

    """
    fpath = os.sep.join(dirs)
    if len(dirs[-1].split(".")) == 1:
        # Not a file
        fpath += os.sep

    return fpath

# --- Base directory lists ----------------------------------------------------
root_dirs = ["%sprj" % os.sep, "nrfa_dev", "NCEA"]
input_dirs = root_dirs + ["input_files"]
metadata_dirs = root_dirs + ["metadata"]
output_dirs = root_dirs + ["output_files"]
raw_dirs = root_dirs + ["raw_data"]

RAW_DATA_DIR = make_fpath(root_dirs + ["raw_data"])
OUTPUT_DIR = make_fpath(root_dirs + ["output_files"])


# --- Catchment descriptor paths ----------------------------------------------
# Directories
catch_dirs = raw_dirs + ["catchment"]
GB_FEH_DIR = make_fpath(catch_dirs + ["tifs", "FEH", "GB"])
NI_FEH_DIR = make_fpath(catch_dirs + ["tifs", "FEH", "NI"])
GB_LCM_2000_DIR = make_fpath(catch_dirs + ["tifs", "LCM", "GB", "2000"])
NI_LCM_2000_DIR = make_fpath(catch_dirs + ["tifs", "LCM", "NI", "2000"])
GB_LCM_2007_DIR = make_fpath(catch_dirs + ["tifs", "LCM", "GB", "2007"])
NI_LCM_2007_DIR = make_fpath(catch_dirs + ["tifs", "LCM", "NI", "2007"])
GB_LCM_2015_DIR = make_fpath(catch_dirs + ["tifs", "LCM", "GB", "2015"])
NI_LCM_2015_DIR = make_fpath(catch_dirs + ["tifs", "LCM", "NI", "2015"])
QCN_DIR = make_fpath(catch_dirs + ["csvs"])
CCAR_DIR = make_fpath(catch_dirs + ["tifs", "CCAR"])

# Files
CCAR_FILE = make_fpath([CCAR_DIR, "CCAR.tif"])
TEMP_CCAR_FILE = make_fpath([CCAR_DIR, "temp_CCAR.tif"])


# --- EA Water quality paths --------------------------------------------------
# Directories
# Raw
EA_WQ_WIMS_RAW_DIR = make_fpath(raw_dirs + ["EA_Water_Quality", "WIMS"])
EA_WQ_IHU_RAW_DIR = make_fpath(raw_dirs + ["EA_Water_Quality", "IHU"])
EA_WQ_WFD_RAW_DIR = make_fpath(raw_dirs + ["EA_Water_Quality", "WFD"])
# Output
EA_WQ_WIMS_OUTPUT_DIR = make_fpath(output_dirs + ["EA_Water_Quality", "WIMS"])

# URLs
EA_WQ_WEBSITE = "https://environment.data.gov.uk/water-quality/view/landing"
EA_WQ_BATCH_URL = "https://environment.data.gov.uk/water-quality/batch/measurement"
EA_WQ_API_URL = "https://environment.data.gov.uk/water-quality"
EA_WQ_ID_URL = "http://environment.data.gov.uk/water-quality"


# --- EA Bio paths ------------------------------------------------------------
# URLs
EA_ECO_BASE_URL = "https://environment.data.gov.uk/ecology/api/v1/"
EA_BIO_WEBSITE = "https://environment.data.gov.uk/ecology/explorer/"


# --- EA Fish paths -----------------------------------------------------------
# Files
EA_FISH_RAW_FILE = make_fpath(raw_dirs + ["EA_Fish", "FW_Fish_Counts.csv"])

# URLs
EA_FISH_WEBSITE = "https://environment.data.gov.uk/ecology/explorer/"


# --- EA Macroinvertebrates ---------------------------------------------------
# Files
EA_INV_METRICS_RAW_FILE = make_fpath(raw_dirs + ["EA_Macroinvertebrates",
                                                 "INV_OPEN_DATA_METRICS.csv"])
EA_INV_SITE_RAW_FILE = make_fpath(raw_dirs + ["EA_Macroinvertebrates",
                                              "INV_OPEN_DATA_SITE.csv"])


# --- EA Macrophytes ----------------------------------------------------------
# Files
EA_MACP_METRICS_RAW_FILE = make_fpath(raw_dirs + ["EA_Macrophyte",
                                                  "MACP_OPEN_DATA_METRICS.csv"])
EA_MACP_SITE_RAW_FILE = make_fpath(raw_dirs + ["EA_Macrophyte",
                                               "MACP_OPEN_DATA_SITE.csv"])


# --- EA Diatom ---------------------------------------------------------------
# Files
EA_DIAT_METRICS_RAW_FILE = make_fpath(raw_dirs + ["EA_Diatom",
                                                  "DIAT_OPEN_DATA_METRICS.csv"])
EA_DIAT_SITE_RAW_FILE = make_fpath(raw_dirs + ["EA_Diatom",
                                               "DIAT_OPEN_DATA_SITE.csv"])


# --- Riverflies --------------------------------------------------------------
# Directories
# Raw
RF_RAW_DIR = make_fpath(raw_dirs + ["RiverFlies", "catchments"])

# URLs
RF_WEBSITE = "https://www.riverflies.org/"


# --- Fresh water watch -------------------------------------------------------
# Files
FWW_RAW_FILE = make_fpath(raw_dirs+ ["FreshWater_Watch",
                                     "fww-dataset-latest.csv"])

# URLs
FWW_WEBSITE = "https://freshwaterwatch.thewaterhub.org/"


# --- NRFA --------------------------------------------------------------------
# Files
NRFA_CDR_DATA_FILE = make_fpath(raw_dirs+ ["NRFA",
                                           "mean_nrfa_cdr_rainfall.csv"])
NRFA_AMAX_DATA_FILE = make_fpath(raw_dirs+ ["NRFA",
                                            "mean_nrfa_amax_flow.csv"])

# URLs
NRFA_API_URL = "https://nrfaapps.ceh.ac.uk/nrfa/ws"
NRFA_WEBSITE = "https://nrfa.ceh.ac.uk/"




# --- Metadata ----------------------------------------------------------------
METADATA_CSV_DIR = make_fpath(metadata_dirs + ["csvs"])
METADATA_AVAIL_JSON_DIR = make_fpath(metadata_dirs + ["json", "availability"])
METADATA_DTYPE_JSON_DIR = make_fpath(metadata_dirs + ["json", "dtypes"])
METADATA_AREA_JSON_DIR = make_fpath(metadata_dirs + ["json", "areas"])

METADATA_NETWORK_JSON_FILE = make_fpath(metadata_dirs + ["json",
                                                         "network_info.json"])
NETWORK_INFO_FPATH = METADATA_CSV_DIR + "network_info.csv"
DTYPE_REGISTER_FPATH = METADATA_CSV_DIR + "data_type_register_{NETWORK}.csv"
SITE_REGISTER_FPATH = METADATA_CSV_DIR + "site_register_{NETWORK}.csv"
DATA_AVAILABILITY_FPATH = METADATA_CSV_DIR + "data_availability_{NETWORK}.csv"


# --- SAN Output --------------------------------------------------------------
san_FWDE_dirs = ["%s%snerclactdb.nerc-lancaster.ac.uk" % (os.sep, os.sep),
                 "appdev", "appdev", "HYDROLOGY", "FWDE"]

SAN_AVAIL_JSON_DIRS = {
    config.EA_WQ_ID: make_fpath(san_FWDE_dirs + ["EA_water_quality",
                                                 "availability"]),
    config.EA_INV_ID: make_fpath(san_FWDE_dirs + ["EA_invertibrates",
                                                  "availability"]),
    config.EA_MACP_ID: make_fpath(san_FWDE_dirs + ["EA_macrophyte",
                                                   "availability"]),
    config.EA_DIAT_ID: make_fpath(san_FWDE_dirs + ["EA_diatom",
                                                   "availability"]),
    config.EA_FISH_ID: make_fpath(san_FWDE_dirs + ["EA_fish",
                                                   "availability"]),
    config.RF_ID: make_fpath(san_FWDE_dirs + ["riverflies",
                                              "availability"]),
    config.FWW_ID: make_fpath(san_FWDE_dirs + ["fww",
                                               "availability"]),
    config.NRFA_ID: make_fpath(san_FWDE_dirs + ["nrfa",
                                                "availability"]),
}

# --- Other -------------------------------------------------------------------
# URLs
EIP_API_URL = "https://eip.ceh.ac.uk/hydrology-ukscape/"
