# -*- coding: utf-8 -*-
import paths
import config
import pandas as pd
import catchment_tools as c_tools



data = pd.read_csv(paths.DATA_AVAILABILITY_FPATH.format(NETWORK="EA_WQ_OLD"),
                   dtype={"DTYPE_ID": str})
data = data.drop_duplicates()
data.to_csv(paths.DATA_AVAILABILITY_FPATH.format(
                 NETWORK="EA_WQ"),
             na_rep=None,
             index=False,
             date_format=config.DATE_FORMAT)


data = pd.read_csv(paths.SITE_REGISTER_FPATH.format(NETWORK="EA_WQ_OLD"),
                   dtype={"SITE_ID": str})
data = data.drop_duplicates()
data.to_csv(paths.SITE_REGISTER_FPATH.format(
                 NETWORK="EA_WQ"),
             na_rep=None,
             index=False,
             date_format=config.DATE_FORMAT)
import pdb; pdb.set_trace()


#paths.DATA_AVAILABILITY_FPATH



easting = 80100
northing = 480500

catch = c_tools.CatchmentData(easting, northing, snap_to_river=True)
data = catch.get_data()
