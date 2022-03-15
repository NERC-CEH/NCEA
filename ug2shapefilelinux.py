# -*- coding: utf-8 -*-
import sys
import geopandas

def convert_to_shp(input_file):
    a = geopandas.read_file(input_file)
    shp_filename = input_file[:-3] + ".shp"
    a.to_file(driver = 'ESRI Shapefile', filename = shp_filename)

if __name__ == "__main__":
    print(sys.argv)
    convert_to_shp(sys.argv[1])
    print("File converted to ESRI shapefile format")
