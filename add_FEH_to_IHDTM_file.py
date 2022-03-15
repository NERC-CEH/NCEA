# -*- coding: utf-8 -*-
from catchment_tools import CatchmentData
import paths

import pandas as pd

inp_fname = "20220222_stations_with_LCM2015.csv"
out_fname = "20220222_stations_with_LCM2015_and_FEH.csv"

catchment_df = pd.read_csv(paths.INPUT_DIR + inp_fname)
FEH_df = None

for index, row in catchment_df.iterrows():

    easting = row["IHDTM_Easting_200"]
    northing = row["IHDTM_Northing_200"]

    catch = CatchmentData(easting, northing)
    FEH_data = catch.get_FEH_data(convert_codes=True)

    FEH_row = pd.DataFrame([FEH_data["PROPERTY_VALUE"].values],
                           columns=FEH_data["PROPERTY_ITEM"].values,
                           index=[index])
    if FEH_df is None:
        FEH_df = FEH_row
    else:
        FEH_df = FEH_df.append(FEH_row)

catchment_df = catchment_df.join(FEH_df)
catchment_df.to_csv(paths.OUTPUT_DIR + out_fname, index=False)
