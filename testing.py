# -*- coding: utf-8 -*-
import catchment_tools as c_tools

easting = 80100
northing = 480500

max_ccar, easting, northing = c_tools.read_ccar_nearest_area(easting, northing, 200)
print(max_ccar, easting, northing)

#catch = CatchmentData(easting, northing, station=33206)
#desc_data = catch.get_FEH_data(convert_codes=True)
