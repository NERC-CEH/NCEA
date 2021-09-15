import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import numpy.ma as ma
import pandas as pd
import rasterio as rio
from rasterio.plot import plotting_extent
import geopandas as gpd
import time

# Rasterstats contains the zonalstatistics function 
# that you will use to extract raster values
import rasterstats as rs
import earthpy as et
import earthpy.plot as ep

def root_path():
    #Path where you python code resides
    root_path = "/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa"
    os.chdir(root_path)

def choose_folder(country):
    # Use different folder for GB and NI. This way you won't mix different
    # coordinate systems and datasets.
    # tiff folders are relative to execution path
    root_path()
    try:
        if country == "gb":
            os.chdir("./GB_CONVERTED_TIFFS")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()
            type(working_directory)

        elif country =="ni":
            os.chdir("./NI_CONVERTED_TIFFS")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "QCN":
            os.chdir("./NRFA_catchments_QCNE_QCNN")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "lcm2015_gb":
            os.chdir("./LCM/GB/2015")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "lcm2015_ni":
            os.chdir("./LCM/NI/LCM2015")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "lcm2007_gb":
            os.chdir("./LCM/GB/2007")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "lcm2007_ni":
            os.chdir("./LCM/NI/LCM2007")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "lcm2000_gb":
            os.chdir("./LCM/GB/2000")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()

        elif country == "lcm2000_ni":
            os.chdir("./LCM/NI/LCM2000")
            #print("Switching to working directory: {}.".format(os.getcwd()))
            working_directory = os.getcwd()
        else:
           raise  ValueError("You have to set a non valid country/type")
    except (ValueError, IndexError):
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return working_directory

def read_raster(x,y, directory):
    '''x = easting in meters
       y = northing in meters
       directory = '''
    #create a pandas dataframe to hold the data
    df = pd.DataFrame(columns=['FEH_Code', 'FEH_value'])
    for dir_path, dir_name_list, file_name_list in os.walk(directory):
        for file_name in file_name_list:
            # If this is not a tif file
            if not file_name.endswith('.tif'):
                # Skip it
                continue
            file_path = os.path.join(dir_path, file_name)
            feh_raster = rio.open(file_path)
            #Get the feh descriptor code
            feh_code = str(file_path [-11:-7])

            #Use the provided coordinates
            #TODO: check out of bounds in a separate definition
            coordinates = [(x,y)]
            #print(coordinates)
            #Get the value of raster
            pnts = [sample[0] for sample in feh_raster.sample(coordinates)]
            #Convert list to string
            pnts = str(pnts[0])
            #Create a list to use to append the data to the dataframe
            values_list = [feh_code, pnts]
            #Make a series to make a row
            value_series = pd.Series(values_list, index = df.columns)
            df = df.append(value_series, ignore_index=True)
            feh_raster.close()
    return df

def read_raster_lcm(x,y, directory):
    '''x = easting in meters
       y = northing in meters
       directory = '''
    #create a pandas dataframe to hold the data
    df = pd.DataFrame(columns=['FEH_Code', 'FEH_value'])
    for dir_path, dir_name_list, file_name_list in os.walk(directory):
        #print(dir_path)
        for file_name in file_name_list:
            # If this is not a tif file
            if not file_name.endswith('.tif'):
                # Skip it
                continue
            file_path = os.path.join(dir_path, file_name)
            feh_raster = rio.open(file_path)
            #Get the lcm descriptor code. It is a combination of year 
            # eg (2015) _ and code eg(23) = 2015_23

            lcm_year = str(file_name.split("_")[1])
            #print(lcm_year)
            lcm_code = str(file_name.split("_")[2])
            lcm_code = lcm_code[:-4]
            feh_code = lcm_year + "_" + lcm_code

            #Use the provided coordinates
            #TODO: check out of bounds in a separate definition
            coordinates = [(x,y)]
            #print(coordinates)
            #Get the value of raster
            pnts = [sample[0] for sample in feh_raster.sample(coordinates)]
            #Convert list to string
            pnts = str(pnts[0])
            #Create a list to use to append the data to the dataframe
            values_list = [feh_code, pnts]
            #Make a series to make a row
            value_series = pd.Series(values_list, index = df.columns)
            df = df.append(value_series, ignore_index=True)
            feh_raster.close()
    return df 

def get_descriptors(x,y,country):
    """retrieves descriptors from various raster files"""
    try:
        if country == "gb":
            working_folder = choose_folder(country)
            descriptor_values = read_raster(x,y, working_folder)
            #print(type(descriptor_values))
            #print(descriptor_values)
            return(descriptor_values)

        elif country == "ni":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster(x,y, working_folder)
            return(descriptor_values)
        
        elif country == "lcm2015_gb":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster(x,y, working_folder)
            return(descriptor_values)
        
        elif country == "lcm2015_ni":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster(x,y, working_folder)
            return(descriptor_values)
        
        
        else:
           raise  ValueError("Please give coordinates for calculations as follows:\
           x (easting), y(northing) in meters.")

    except:
       print("Unexpected error:", sys.exc_info()[0])

def get_descriptors_lcm(x,y,country):
    """retrieves descriptors from various raster files"""
    try:
        
        if country == "lcm2015_gb":
            working_folder = choose_folder(country)
            read_raster_lcm(x,y, working_folder)
            descriptor_values = read_raster_lcm(x,y, working_folder)
            return(descriptor_values)
        
        elif country == "lcm2015_ni":
            working_folder = choose_folder(country)
            read_raster_lcm(x,y, working_folder)
            descriptor_values = read_raster_lcm(x,y, working_folder)
            return(descriptor_values)

        elif country == "lcm2007_gb":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster_lcm(x,y, working_folder)
            return(descriptor_values)
        
        elif country == "lcm2007_ni":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster_lcm(x,y, working_folder)
            return(descriptor_values)

        elif country == "lcm2000_gb":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster_lcm(x,y, working_folder)
            return(descriptor_values)
        
        elif country == "lcm2000_ni":
            working_folder = choose_folder(country)
            read_raster(x,y, working_folder)
            descriptor_values = read_raster_lcm(x,y, working_folder)
            return(descriptor_values)

        else:
           raise  ValueError("Please give coordinates for calculations as follows:\
           x (easting), y(northing) in meters.")

    except:
       print("Unexpected error:", sys.exc_info()[0])

'''IMPORTANT NOTE: This definitions was used to simulate the way we capture values
from different raster datasets (1km). 
Usage: checks if coordinate fall exactly on the border of two 1km cells. If yes,
it adds 1m to the coordinate in order to get the rightmost value for easting or
the northmost for northing.
I used this def to simulate the way FEH values derived and stored in Oracle. There
is no documentation about how this was done in the past. Matt Fry suggested that
we should follow WHS method: when we have a point placed exactly on the border of
two raster cells we read the left value for easting and lower value for northing.
This applies only to the 1km datasets. For the 50m datasets there is no such problem 
since we choose all our point to be rounded in the next 50m value. This way,
every point is in the centre of a raster cell and it reads the value of this cell.'''
def fixRMD_easting(num):
    div = (num // 500) % 2
    #print("Div: ", div)
    rem = num % 500
    #print("rem: ", rem)
    if rem == 0 and div != 0:
        #print("calc new number")
        #new_num = num + 1
        new_num = num - 1
    else:
        new_num = num
    return new_num

def fixRMD_northing(num):
    div = (num // 500) % 2
    #print("Div: ", div)
    rem = num % 500
    #print("rem: ", rem)
    if rem == 0 and div != 0:
        #print("calc new number")
        new_num = num + 1
        #new_num = num
    else:
        new_num = num
    return new_num

if __name__ == "__main__":
        
    #Open a csv file with coordinates and station numbers (from Oracle)
    #GB Dataset
    #stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_GB_ONLY.csv")
    #stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/irn_lines_HA72_mdns_VAS.csv")
    #stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/EASTMAN_POINTS.csv")
    
    
    '''#Loop through stations to get the FEH descriptors for GB
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/\
    TESTING_WITH_ORACLE/STATION_SPATIAL_GB_ONLY.csv")
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "gb"
        
        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors( easting, northing, country)

        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "FEH"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        
        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT" ]]
        
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/FEH_{0}_{1}.csv".format(station_name, now))'''
    
    
    
    #NI Dataset 
    #stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_NI_ONLY.csv")
    #stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_69016_SPATIAL.csv")
    
    '''#Loop through stations to get the FEH descriptors for NI
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/\
    TESTING_WITH_ORACLE/STATION_SPATIAL_NI_ONLY.csv")
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "ni"

        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors( easting, northing, country)

        #convert column to flot in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all "null" values
        output_file = output_file[output_file.FEH_value > -500]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "FEH"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        
        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT" ]]
        
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/FEH_{0}_{1}.csv".format(station_name, now))
    '''
    
    
    #QCN (Polygon centroids) Dataset
    '''Centroids csv file is a direct export from the centroid calculation in ArcGIS.
    For every new catchment you have to re-export this file after adding the new 
    catchment
    If it's only one station may it's quicker to copy and paste the coordinated
    directly from ArcGIS...
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/\
    NRFA_catchments_QCNE_QCNN/catchments_all.csv")

    qcndf = pd.DataFrame(columns=['STATION', 'PROPERTY_ITEM', 'PROPERTY_VALUE'])
    #Loop through 
    QCN_dataframe = pd.DataFrame()
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        QCNE = int(row['QCNE'])
        QCNN = int(row['QCNN'])
        list_qcne = [station_name, 'QCNE', QCNE]
        list_qcnn = [station_name, 'QCNN', QCNN]
        
        #Make a series per row
        value_series_qcne = pd.Series(list_qcne, index = qcndf.columns)
        value_series_qcnn = pd.Series(list_qcnn, index = qcndf.columns)

        qcndf = qcndf.append(value_series_qcne,  ignore_index= True) 
        qcndf = qcndf.append(value_series_qcnn,  ignore_index= True)
        
    #Add Columns to match NRFA Oracle table
    qcndf["PROPERTY_GROUP"] = "FEH"
    qcndf["PROPERTY_METHOD"] = "automatic"
    qcndf["PROPERTY_COMMENT"] = ""


    #rearrange columns to match Oracle table
    qcndf = qcndf[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT" ]]
    
    now = int(time.time())

    qcndf.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/\
    TESTING_WITH_ORACLE/CSV/FEH_{0}_{1}.csv".format("QCNE_QCNN", now))'''

    '''#Loop through stations to get the LCM2015 for GB
    #stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_GB_ONLY.csv")
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "lcm2015_gb"
        
        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors_lcm( easting, northing, country) #<class 'pandas.core.frame.DataFrame'>
        #print(type(output_file))
        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        #output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "lcm2015v2021"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        output_file["TITLE"] = ""
        output_file["UNITS"] = "proportion"
        output_file["SOURCE_VALUE"] = output_file["FEH_Code"]
        #Divide by 400 to get the area in square km
        output_file["FEH_value"] = output_file["FEH_value"] / 400

        # Get the sum of area
        catchment_area = output_file["FEH_value"].sum()
        #print(catchment_area)
        #print(type(catchment_area))

        #Calculate the percentage of each category
        output_file["FEH_value"] = (output_file["FEH_value"]/catchment_area) * 100

        #Create nrfa grouped categories
        #NRFA woodland = LCM categories 1+2
        nrfa_woodland = output_file.query("FEH_Code =='2015_1'or \
        FEH_Code =='2015_2'\
         ")["FEH_value"].sum()
        
        #NRFA arable and horticulture = LCM categories 3
        nrfa_arable = output_file.query("FEH_Code =='2015_3'\
        ")["FEH_value"].sum()

        #NRFA grassland = LCM categories 4+5+6+7+8
        nrfa_grassland = output_file.query("FEH_Code =='2015_4' or \
         FEH_Code =='2015_5' or \
         FEH_Code =='2015_6' or \
         FEH_Code =='2015_7' or \
         FEH_Code =='2015_8'\
         ")["FEH_value"].sum()

        #NRFA heath/bog = LCM categories 9+10+11
        nrfa_heath_bog = output_file.query("FEH_Code =='2015_9' or \
         FEH_Code =='2015_10' or \
         FEH_Code =='2015_11'\
         ")["FEH_value"].sum()

        #NRFA inland rock = LCM categories 12
        nrfa_inland = output_file.query("FEH_Code =='2015_12' \
         ")["FEH_value"].sum()

        #NRFA water = LCM categories 13+14
        nrfa_water = output_file.query("FEH_Code =='2015_13' or  \
        FEH_Code =='2015_14' \
         ")["FEH_value"].sum()

        #NRFA coastal = LCM categories 15+16+17+18+19
        nrfa_coastal = output_file.query("FEH_Code =='2015_15' or \
        FEH_Code =='2015_16' or \
        FEH_Code =='2015_17' or \
        FEH_Code =='2015_18' or \
        FEH_Code =='2015_19' \
         ")["FEH_value"].sum()

        #NRFA urban = LCM categories 20+21
        nrfa_urban = output_file.query("FEH_Code =='2015_20' or \
        FEH_Code =='2015_21' \
         ")["FEH_value"].sum()

        #NRFA uknown = LCM categories -9999
        nrfa_unknown = output_file.query("FEH_Code =='2015_9999'\
         ")["FEH_value"].sum()



        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT",\
                        "TITLE", "UNITS", "SOURCE_VALUE" ]]

        #Append rows with values
        #Get the max index value first
        index_no = output_file.index.max()

        output_file.loc[index_no+1] = ([station_name, "lcm2015nrfav2021" , "Woodland"\
        , nrfa_woodland, "automatic", "", "Woodland", "proportion", "1+2" ])  

        output_file.loc[index_no+2] = ([station_name, "lcm2015nrfav2021" , "Arable and Horticulture"\
        , nrfa_arable, "automatic", "", "Arable and Horticulture", "proportion", "3" ])

        output_file.loc[index_no+3] = ([station_name, "lcm2015nrfav2021" , "Grassland"\
        , nrfa_grassland, "automatic", "", "Grassland", "proportion", "4+5+6+7+8"])

        output_file.loc[index_no+4] = ([station_name, "lcm2015nrfav2021" , "Heath/Bog"\
        , nrfa_heath_bog, "automatic", "", "Heath/Bog", "proportion", "9+10+11" ])

        output_file.loc[index_no+5] = ([station_name, "lcm2015nrfav2021" , "Inland Rock"\
        , nrfa_inland, "automatic", "", "Inland Rock", "proportion", "12" ])

        output_file.loc[index_no+6] = ([station_name, "lcm2015nrfav2021" , "Water"\
        , nrfa_water, "automatic", "", "Water", "proportion", "13+14" ])

        output_file.loc[index_no+7] = ([station_name, "lcm2015nrfav2021" , "Coastal"\
        , nrfa_coastal, "automatic", "", "Coastal", "proportion", "15+16+17+18+19" ])

        output_file.loc[index_no+8] = ([station_name, "lcm2015nrfav2021" , "Urban"\
        , nrfa_urban, "automatic", "", "Urban", "proportion", "20+21" ])

        output_file.loc[index_no+9] = ([station_name, "lcm2015nrfav2021" , "Unknown"\
        , nrfa_unknown, "automatic", "", "Unknown", "proportion", "9999" ])
                
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/LCM2015_{0}_{1}.csv".format(station_name, now))'''


    '''#Loop through stations to get the LCM2015 for NI
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_NI_ONLY.csv")
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "lcm2015_ni"
        
        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors_lcm( easting, northing, country) #<class 'pandas.core.frame.DataFrame'>
        #print(output_file)
        #print(type(output_file))
        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        #output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "lcm2015v2021"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        output_file["TITLE"] = ""
        output_file["UNITS"] = "proportion"
        output_file["SOURCE_VALUE"] = output_file["FEH_Code"]
        
        #Divide by 400 to get the area in square km
        output_file["FEH_value"] = output_file["FEH_value"] / 400

        # Get the sum of area
        catchment_area = output_file["FEH_value"].sum()
        #print(catchment_area)
        #print(type(catchment_area))

        #Calculate the percentage of each category
        output_file["FEH_value"] = (output_file["FEH_value"]/catchment_area) * 100

        #Create nrfa grouped categories
        #NRFA woodland = LCM categories 1+2
        nrfa_woodland = output_file.query("FEH_Code =='2015_1'or \
        FEH_Code =='2015_2'\
         ")["FEH_value"].sum()
        
        #NRFA arable and horticulture = LCM categories 3
        nrfa_arable = output_file.query("FEH_Code =='2015_3'\
        ")["FEH_value"].sum()

        #NRFA grassland = LCM categories 4+5+6+7+8
        nrfa_grassland = output_file.query("FEH_Code =='2015_4' or \
         FEH_Code =='2015_5' or \
         FEH_Code =='2015_6' or \
         FEH_Code =='2015_7' or \
         FEH_Code =='2015_8'\
         ")["FEH_value"].sum()

        #NRFA heath/bog = LCM categories 9+10+11
        nrfa_heath_bog = output_file.query("FEH_Code =='2015_9' or \
         FEH_Code =='2015_10' or \
         FEH_Code =='2015_11'\
         ")["FEH_value"].sum()

        #NRFA inland rock = LCM categories 12
        nrfa_inland = output_file.query("FEH_Code =='2015_12' \
         ")["FEH_value"].sum()

        #NRFA water = LCM categories 13+14
        nrfa_water = output_file.query("FEH_Code =='2015_13' or  \
        FEH_Code =='2015_14' \
         ")["FEH_value"].sum()

        #NRFA coastal = LCM categories 15+16+17+18+19
        nrfa_coastal = output_file.query("FEH_Code =='2015_15' or \
        FEH_Code =='2015_16' or \
        FEH_Code =='2015_17' or \
        FEH_Code =='2015_18' or \
        FEH_Code =='2015_19' \
         ")["FEH_value"].sum()

        #NRFA urban = LCM categories 20+21
        nrfa_urban = output_file.query("FEH_Code =='2015_20' or \
        FEH_Code =='2015_21' \
         ")["FEH_value"].sum()

        #NRFA uknown = LCM categories -9999
        nrfa_unknown = output_file.query("FEH_Code =='2015_9999'\
         ")["FEH_value"].sum()



        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT",\
                        "TITLE", "UNITS", "SOURCE_VALUE" ]]

        #Append rows with values
        #Get the max index value first
        index_no = output_file.index.max()

        output_file.loc[index_no+1] = ([station_name, "lcm2015nrfav2021" , "Woodland"\
        , nrfa_woodland, "automatic", "", "Woodland", "proportion", "1+2" ])  

        output_file.loc[index_no+2] = ([station_name, "lcm2015nrfav2021" , "Arable and Horticulture"\
        , nrfa_arable, "automatic", "", "Arable and Horticulture", "proportion", "3" ])

        output_file.loc[index_no+3] = ([station_name, "lcm2015nrfav2021" , "Grassland"\
        , nrfa_grassland, "automatic", "", "Grassland", "proportion", "4+5+6+7+8"])

        output_file.loc[index_no+4] = ([station_name, "lcm2015nrfav2021" , "Heath/Bog"\
        , nrfa_heath_bog, "automatic", "", "Heath/Bog", "proportion", "9+10+11" ])

        output_file.loc[index_no+5] = ([station_name, "lcm2015nrfav2021" , "Inland Rock"\
        , nrfa_inland, "automatic", "", "Inland Rock", "proportion", "12" ])

        output_file.loc[index_no+6] = ([station_name, "lcm2015nrfav2021" , "Water"\
        , nrfa_water, "automatic", "", "Water", "proportion", "13+14" ])

        output_file.loc[index_no+7] = ([station_name, "lcm2015nrfav2021" , "Coastal"\
        , nrfa_coastal, "automatic", "", "Coastal", "proportion", "15+16+17+18+19" ])

        output_file.loc[index_no+8] = ([station_name, "lcm2015nrfav2021" , "Urban"\
        , nrfa_urban, "automatic", "", "Urban", "proportion", "20+21" ])

        output_file.loc[index_no+9] = ([station_name, "lcm2015nrfav2021" , "Unknown"\
        , nrfa_unknown, "automatic", "", "Unknown", "proportion", "9999" ])
                
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/LCM2015_{0}_{1}.csv".format(station_name, now))'''

    #Loop through stations to get the LCM2007 for GB
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_GB_ONLY.csv")
    tot_stations = len(stations_df)
    for num, (index, row) in enumerate(stations_df.iterrows()):
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "lcm2007_gb"

        print("GB %s - %s/%s" % (station_name, num+1, tot_stations))
        
        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors_lcm( easting, northing, country) #<class 'pandas.core.frame.DataFrame'>

        #print(type(output_file))
        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        #output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "lcm2007v2021"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        output_file["TITLE"] = ""
        output_file["UNITS"] = "proportion"
        output_file["SOURCE_VALUE"] = output_file["FEH_Code"]

        #Divide by 400 to get the area in square km
        output_file["FEH_value"] = output_file["FEH_value"] / 400

        # Get the sum of area
        catchment_area = output_file["FEH_value"].sum()
        #print(catchment_area)
        #print(type(catchment_area))

        #Calculate the percentage of each category
        output_file["FEH_value"] = (output_file["FEH_value"]/catchment_area) * 100

        #Create nrfa grouped categories
        #NRFA woodland = LCM categories 1+2
        nrfa_woodland = output_file.query("FEH_Code =='2007_1'or \
        FEH_Code =='2007_2'\
         ")["FEH_value"].sum()
        
        #NRFA arable and horticulture = LCM categories 3
        nrfa_arable = output_file.query("FEH_Code =='2007_3'\
        ")["FEH_value"].sum()

        #NRFA grassland = LCM categories 4+5+6+7+8+9
        nrfa_grassland = output_file.query("FEH_Code =='2007_4' or \
         FEH_Code =='2007_5' or \
         FEH_Code =='2007_6' or \
         FEH_Code =='2007_7' or \
         FEH_Code =='2007_8' or \
         FEH_Code =='2007_9' \
         ")["FEH_value"].sum()

        #NRFA heath/bog = LCM categories 10+11+12+13
        nrfa_heath_bog = output_file.query("FEH_Code =='2007_10' or \
         FEH_Code =='2007_11' or \
         FEH_Code =='2007_12' or \
         FEH_Code =='2007_13' \
         ")["FEH_value"].sum()

        #NRFA inland rock = LCM categories 14
        nrfa_inland = output_file.query("FEH_Code =='2007_14' \
         ")["FEH_value"].sum()

        #NRFA water = LCM categories 15+16
        nrfa_water = output_file.query("FEH_Code =='2007_15' or  \
        FEH_Code =='2007_16' \
         ")["FEH_value"].sum()

        #NRFA coastal = LCM categories 17+18+19+20+21
        nrfa_coastal = output_file.query("FEH_Code =='2007_17' or \
        FEH_Code =='2007_18' or \
        FEH_Code =='2007_19' or \
        FEH_Code =='2007_20' or \
        FEH_Code =='2007_21' \
         ")["FEH_value"].sum()

        #NRFA urban = LCM categories 22+23
        nrfa_urban = output_file.query("FEH_Code =='2007_22' or \
        FEH_Code =='2007_23' \
         ")["FEH_value"].sum()

        #NRFA uknown = LCM categories -9999
        nrfa_unknown = output_file.query("FEH_Code =='2007_9999'\
         ")["FEH_value"].sum()



        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT",\
                        "TITLE", "UNITS", "SOURCE_VALUE" ]]

        #Append rows with values
        #Get the max index value first
        index_no = output_file.index.max()

        output_file.loc[index_no+1] = ([station_name, "lcm2007nrfav2021" ,\
        "Woodland", nrfa_woodland, "automatic", "", "Woodland",\
        "proportion","1+2"])  

        output_file.loc[index_no+2] = ([station_name, "lcm2007nrfav2021" ,\
        "Arable and Horticulture", nrfa_arable, "automatic", "",\
        "Arable and Horticulture", "proportion", "3" ])

        output_file.loc[index_no+3] = ([station_name, "lcm2007nrfav2021" ,\
        "Grassland", nrfa_grassland, "automatic", "", "Grassland",\
        "proportion", "4+5+6+7+8+9" ])

        output_file.loc[index_no+4] = ([station_name, "lcm2007nrfav2021" ,\
        "Heath/Bog", nrfa_heath_bog, "automatic", "", "Heath/Bog",\
        "proportion", "10+11+12+13" ])

        output_file.loc[index_no+5] = ([station_name, "lcm2007nrfav2021", \
        "Inland Rock", nrfa_inland, "automatic", "", "Inland Rock", \
        "proportion", "14" ])

        output_file.loc[index_no+6] = ([station_name, "lcm2007nrfav2021" ,\
        "Water", nrfa_water, "automatic", "", "Water", \
         "proportion", "15+16" ])

        output_file.loc[index_no+7] = ([station_name, "lcm2007nrfav2021" ,\
        "Coastal", nrfa_coastal, "automatic", "", "Coastal",\
        "proportion", "17+18+19+20+21" ])

        output_file.loc[index_no+8] = ([station_name, "lcm2007nrfav2021" ,\
        "Urban", nrfa_urban, "automatic", "", "Urban", \
        "proportion", "22+23" ])

        output_file.loc[index_no+9] = ([station_name, "lcm2007nrfav2021" , \
        "Unknown", nrfa_unknown, "automatic", "", "Uknown", \
        "proportion", "9999" ])
                
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/LCM2007_{0}_{1}.csv".format(station_name, now))
        
           
    #Loop through stations to get the LCM2007 for NI
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_NI_ONLY.csv")
    tot_stations = len(stations_df)
    for num, (index, row) in enumerate(stations_df.iterrows()):
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "lcm2007_ni"

        print("NI %s - %s/%s" % (station_name, num+1, tot_stations))

        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors_lcm( easting, northing, country) #<class 'pandas.core.frame.DataFrame'>
        #print(output_file)
        #print(type(output_file))
        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        #output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "lcm2007v2021"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        output_file["TITLE"] = ""
        output_file["UNITS"] = "proportion"
        output_file["SOURCE_VALUE"] = output_file["FEH_Code"]

        #Divide by 400 to get the area in square km
        output_file["FEH_value"] = output_file["FEH_value"] / 400

        # Get the sum of area
        catchment_area = output_file["FEH_value"].sum()
        #print(catchment_area)
        #print(type(catchment_area))

        #Calculate the percentage of each category
        output_file["FEH_value"] = (output_file["FEH_value"]/catchment_area) * 100

        #Create nrfa grouped categories
        #NRFA woodland = LCM categories 1+2
        nrfa_woodland = output_file.query("FEH_Code =='2007_1'or \
        FEH_Code =='2007_2'\
         ")["FEH_value"].sum()
        
        #NRFA arable and horticulture = LCM categories 3
        nrfa_arable = output_file.query("FEH_Code =='2007_3'\
        ")["FEH_value"].sum()

        #NRFA grassland = LCM categories 4+5+6+7+8+9
        nrfa_grassland = output_file.query("FEH_Code =='2007_4' or \
         FEH_Code =='2007_5' or \
         FEH_Code =='2007_6' or \
         FEH_Code =='2007_7' or \
         FEH_Code =='2007_8' or \
         FEH_Code =='2007_9' \
         ")["FEH_value"].sum()

        #NRFA heath/bog = LCM categories 10+11+12+13
        nrfa_heath_bog = output_file.query("FEH_Code =='2007_10' or \
         FEH_Code =='2007_11' or \
         FEH_Code =='2007_12' or \
         FEH_Code =='2007_13' \
         ")["FEH_value"].sum()

        #NRFA inland rock = LCM categories 14
        nrfa_inland = output_file.query("FEH_Code =='2007_14' \
         ")["FEH_value"].sum()

        #NRFA water = LCM categories 15+16
        nrfa_water = output_file.query("FEH_Code =='2007_15' or  \
        FEH_Code =='2007_16' \
         ")["FEH_value"].sum()

        #NRFA coastal = LCM categories 17+18+19+20+21
        nrfa_coastal = output_file.query("FEH_Code =='2007_17' or \
        FEH_Code =='2007_18' or \
        FEH_Code =='2007_19' or \
        FEH_Code =='2007_20' or \
        FEH_Code =='2007_21' \
         ")["FEH_value"].sum()

        #NRFA urban = LCM categories 22+23
        nrfa_urban = output_file.query("FEH_Code =='2007_22' or \
        FEH_Code =='2007_23' \
         ")["FEH_value"].sum()

        #NRFA uknown = LCM categories -9999
        nrfa_unknown = output_file.query("FEH_Code =='2007_9999'\
         ")["FEH_value"].sum()



        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT",\
                        "TITLE", "UNITS", "SOURCE_VALUE" ]]

        #Append rows with values
        #Get the max index value first
        index_no = output_file.index.max()

        output_file.loc[index_no+1] = ([station_name, "lcm2007nrfav2021" ,\
        "Woodland", nrfa_woodland, "automatic", "", "Woodland",\
        "proportion","1+2"])  

        output_file.loc[index_no+2] = ([station_name, "lcm2007nrfav2021" ,\
        "Arable and Horticulture", nrfa_arable, "automatic", "",\
        "Arable and Horticulture", "proportion", "3" ])

        output_file.loc[index_no+3] = ([station_name, "lcm2007nrfav2021" ,\
        "Grassland", nrfa_grassland, "automatic", "", "Grassland",\
        "proportion", "4+5+6+7+8+9" ])

        output_file.loc[index_no+4] = ([station_name, "lcm2007nrfav2021" ,\
        "Heath/Bog", nrfa_heath_bog, "automatic", "", "Heath/Bog",\
        "proportion", "10+11+12+13" ])

        output_file.loc[index_no+5] = ([station_name, "lcm2007nrfav2021", \
        "Inland Rock", nrfa_inland, "automatic", "", "Inland Rock", \
        "proportion", "14" ])

        output_file.loc[index_no+6] = ([station_name, "lcm2007nrfav2021" ,\
        "Water", nrfa_water, "automatic", "", "Water", \
         "proportion", "15+16" ])

        output_file.loc[index_no+7] = ([station_name, "lcm2007nrfav2021" ,\
        "Coastal", nrfa_coastal, "automatic", "", "Coastal",\
        "proportion", "17+18+19+20+21" ])

        output_file.loc[index_no+8] = ([station_name, "lcm2007nrfav2021" ,\
        "Urban", nrfa_urban, "automatic", "", "Urban", \
        "proportion", "22+23" ])

        output_file.loc[index_no+9] = ([station_name, "lcm2007nrfav2021" , \
        "Unknown", nrfa_unknown, "automatic", "", "Uknown", \
        "proportion", "9999" ])
                
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/LCM2007_{0}_{1}.csv".format(station_name, now))

    '''#Loop through stations to get the LCM2000 for GB
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_GB_ONLY.csv")
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "lcm2000_gb"
        
        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors_lcm( easting, northing, country) #<class 'pandas.core.frame.DataFrame'>
        #print(type(output_file))
        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        #output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "lcm2000v2021"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        output_file["TITLE"] = ""
        output_file["UNITS"] = "proportion"
        output_file["SOURCE_VALUE"] = output_file["FEH_Code"]

        #Divide by 400 to get the area in square km
        output_file["FEH_value"] = output_file["FEH_value"] / 400

        # Get the sum of area
        catchment_area = output_file["FEH_value"].sum()
        #print(catchment_area)
        #print(type(catchment_area))

        #Calculate the percentage of each category
        output_file["FEH_value"] = (output_file["FEH_value"]/catchment_area) * 100

        #Create nrfa grouped categories
        #NRFA woodland = LCM categories 11+21
        nrfa_woodland = output_file.query("FEH_Code =='2000_11' or \
        FEH_Code =='2000_21'\
         ")["FEH_value"].sum()
        
        #NRFA arable and horticulture = LCM categories 41+42+43
        nrfa_arable = output_file.query("FEH_Code =='2000_41'or \
        FEH_Code =='2000_42' or\
        FEH_Code =='2000_43'\
        ")["FEH_value"].sum()

        #NRFA grassland = LCM categories 51+61+52+91+71+81+111
        nrfa_grassland = output_file.query("FEH_Code =='2000_51' or \
         FEH_Code =='2000_52' or \
         FEH_Code =='2000_61' or \
         FEH_Code =='2000_71' or \
         FEH_Code =='2000_81' or \
         FEH_Code =='2000_91' or \
         FEH_Code =='2000_111' \
         ")["FEH_value"].sum()

        #NRFA heath/bog = LCM categories 101+102+121+151
        nrfa_heath_bog = output_file.query("FEH_Code =='2000_101' or \
         FEH_Code =='2000_102' or \
         FEH_Code =='2000_121' or \
         FEH_Code =='2000_151' \
         ")["FEH_value"].sum()

        #NRFA bare ground (probably Inland Rock) = LCM categories 161, keep same name
        nrfa_inland = output_file.query("FEH_Code =='2000_161' \
         ")["FEH_value"].sum()

        #NRFA water = LCM categories 121+131
        nrfa_water = output_file.query("FEH_Code =='2000_121' or  \
        FEH_Code =='2000_131' \
         ")["FEH_value"].sum()

        #NRFA coastal = LCM categories 181+191+201+211+212
        nrfa_coastal = output_file.query("FEH_Code =='2000_181' or \
        FEH_Code =='2000_191' or \
        FEH_Code =='2000_201' or \
        FEH_Code =='2000_211' or \
        FEH_Code =='2000_212' \
         ")["FEH_value"].sum()

        #NRFA urban = LCM categories 171+172
        nrfa_urban = output_file.query("FEH_Code =='2000_171' or \
        FEH_Code =='2000_172' \
         ")["FEH_value"].sum()

        #NRFA uknown = LCM categories -9999
        nrfa_unknown = output_file.query("FEH_Code =='2000_9999'\
         ")["FEH_value"].sum()



        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT",\
                        "TITLE", "UNITS", "SOURCE_VALUE" ]]

        #Append rows with values
        #Get the max index value first
        index_no = output_file.index.max()

        output_file.loc[index_no+1] = ([station_name, "lcm2000nrfav2021" ,\
        "Woodland", nrfa_woodland, "automatic", "", "Woodland",\
        "proportion","11+21"])  

        output_file.loc[index_no+2] = ([station_name, "lcm2000nrfav2021" ,\
        "Arable and Horticulture", nrfa_arable, "automatic", "",\
        "Arable and Horticulture", "proportion", "41+42+43" ])

        output_file.loc[index_no+3] = ([station_name, "lcm2000nrfav2021" ,\
        "Grassland", nrfa_grassland, "automatic", "", "Grassland",\
        "proportion", "51+52+61+71+81+91+111" ])

        output_file.loc[index_no+4] = ([station_name, "lcm2000nrfav2021" ,\
        "Heath/Bog", nrfa_heath_bog, "automatic", "", "Heath/Bog",\
        "proportion", "101+102+121+151" ])

        output_file.loc[index_no+5] = ([station_name, "lcm2000nrfav2021", \
        "Bareground", nrfa_inland, "automatic", "", "Bareground", \
        "proportion", "161" ])

        output_file.loc[index_no+6] = ([station_name, "lcm2000nrfav2021" ,\
        "Water", nrfa_water, "automatic", "", "Water", \
         "proportion", "131+221" ])

        output_file.loc[index_no+7] = ([station_name, "lcm2000nrfav2021" ,\
        "Coastal", nrfa_coastal, "automatic", "", "Coastal",\
        "proportion", "181+191+201+211+212" ])

        output_file.loc[index_no+8] = ([station_name, "lcm2000nrfav2021" ,\
        "Urban", nrfa_urban, "automatic", "", "Urban", \
        "proportion", "171+172" ])

        output_file.loc[index_no+9] = ([station_name, "lcm2000nrfav2021" , \
        "Unknown", nrfa_unknown, "automatic", "", "Uknown", \
        "proportion", "9999" ])
                
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/LCM2000_{0}_{1}.csv".format(station_name, now))'''
        
           
    '''#Loop through stations to get the LCM2000 for NI
    stations_df = pd.read_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/STATION_SPATIAL_NI_ONLY.csv")
    for index, row in stations_df.iterrows():
        station_name = row['STATION']
        easting = int(row['DTM_EASTING'])
        northing = int(row['DTM_NORTHING'])
        country = "lcm2000_ni"
        
        #fix for RMD1, RMD2, RMD3. Not used any more since we decided to 
        #go with WHS method of deriving valus from datasets. More on def notes.
        easting = fixRMD_easting(easting)
        northing = fixRMD_northing(northing)
        output_file = get_descriptors_lcm( easting, northing, country) #<class 'pandas.core.frame.DataFrame'>
        #print(output_file)
        #print(type(output_file))
        #convert column to float in order to use it in the next line
        output_file["FEH_value"] = output_file["FEH_value"].astype(float)

        #Eliminate all -9999 values
        #output_file = output_file[output_file.FEH_value > -5000]

        #Add Columns to match NRFA Oracle table
        output_file["STATION"] = station_name
        output_file["PROPERTY_GROUP"] = "lcm2007v2021"
        output_file["PROPERTY_METHOD"] = "automatic"
        output_file["PROPERTY_COMMENT"] = ""
        output_file["TITLE"] = ""
        output_file["UNITS"] = "proportion"
        output_file["SOURCE_VALUE"] = output_file["FEH_Code"]

        #Divide by 400 to get the area in square km
        output_file["FEH_value"] = output_file["FEH_value"] / 400

        # Get the sum of area
        catchment_area = output_file["FEH_value"].sum()
        #print(catchment_area)
        #print(type(catchment_area))

        #Calculate the percentage of each category
        output_file["FEH_value"] = (output_file["FEH_value"]/catchment_area) * 100

        #Create nrfa grouped categories
        #NRFA woodland = LCM categories 11+21
        nrfa_woodland = output_file.query("FEH_Code =='2000_11' or \
        FEH_Code =='2000_21'\
         ")["FEH_value"].sum()
        
        #NRFA arable and horticulture = LCM categories 41+42+43
        nrfa_arable = output_file.query("FEH_Code =='2000_41'or \
        FEH_Code =='2000_42' or\
        FEH_Code =='2000_43'\
        ")["FEH_value"].sum()

        #NRFA grassland = LCM categories 51+61+52+91+71+81+111
        nrfa_grassland = output_file.query("FEH_Code =='2000_51' or \
         FEH_Code =='2000_52' or \
         FEH_Code =='2000_61' or \
         FEH_Code =='2000_71' or \
         FEH_Code =='2000_81' or \
         FEH_Code =='2000_91' or \
         FEH_Code =='2000_111' \
         ")["FEH_value"].sum()

        #NRFA heath/bog = LCM categories 101+102+121+151
        nrfa_heath_bog = output_file.query("FEH_Code =='2000_101' or \
         FEH_Code =='2000_102' or \
         FEH_Code =='2000_121' or \
         FEH_Code =='2000_151' \
         ")["FEH_value"].sum()

        #NRFA bare ground (probably Inland Rock) = LCM categories 161, keep same name
        nrfa_inland = output_file.query("FEH_Code =='2000_161' \
         ")["FEH_value"].sum()

        #NRFA water = LCM categories 121+131
        nrfa_water = output_file.query("FEH_Code =='2000_121' or  \
        FEH_Code =='2000_131' \
         ")["FEH_value"].sum()

        #NRFA coastal = LCM categories 181+191+201+211+212
        nrfa_coastal = output_file.query("FEH_Code =='2000_181' or \
        FEH_Code =='2000_191' or \
        FEH_Code =='2000_201' or \
        FEH_Code =='2000_211' or \
        FEH_Code =='2000_212' \
         ")["FEH_value"].sum()

        #NRFA urban = LCM categories 171+172
        nrfa_urban = output_file.query("FEH_Code =='2000_171' or \
        FEH_Code =='2000_172' \
         ")["FEH_value"].sum()

        #NRFA uknown = LCM categories -9999
        nrfa_unknown = output_file.query("FEH_Code =='2000_9999'\
         ")["FEH_value"].sum()

        #rename columns 
        output_file = output_file.rename(columns={'FEH_value': 'PROPERTY_VALUE',\
                                                    'FEH_Code': 'PROPERTY_ITEM'})

        #rearrange columns to match Oracle table
        output_file = output_file[["STATION","PROPERTY_GROUP",'PROPERTY_ITEM',\
                        'PROPERTY_VALUE',"PROPERTY_METHOD", "PROPERTY_COMMENT",\
                        "TITLE", "UNITS", "SOURCE_VALUE" ]]

        #Append rows with values
        #Get the max index value first
        index_no = output_file.index.max()

        output_file.loc[index_no+1] = ([station_name, "lcm2000nrfav2021" ,\
        "Woodland", nrfa_woodland, "automatic", "", "Woodland",\
        "proportion","11+21"])  

        output_file.loc[index_no+2] = ([station_name, "lcm2000nrfav2021" ,\
        "Arable and Horticulture", nrfa_arable, "automatic", "",\
        "Arable and Horticulture", "proportion", "41+42+43" ])

        output_file.loc[index_no+3] = ([station_name, "lcm2000nrfav2021" ,\
        "Grassland", nrfa_grassland, "automatic", "", "Grassland",\
        "proportion", "51+52+61+71+81+91+111" ])

        output_file.loc[index_no+4] = ([station_name, "lcm2000nrfav2021" ,\
        "Heath/Bog", nrfa_heath_bog, "automatic", "", "Heath/Bog",\
        "proportion", "101+102+121+151" ])

        output_file.loc[index_no+5] = ([station_name, "lcm2000nrfav2021", \
        "Bareground", nrfa_inland, "automatic", "", "Bareground", \
        "proportion", "161" ])

        output_file.loc[index_no+6] = ([station_name, "lcm2000nrfav2021" ,\
        "Water", nrfa_water, "automatic", "", "Water", \
         "proportion", "131+221" ])

        output_file.loc[index_no+7] = ([station_name, "lcm2000nrfav2021" ,\
        "Coastal", nrfa_coastal, "automatic", "", "Coastal",\
        "proportion", "181+191+201+211+212" ])

        output_file.loc[index_no+8] = ([station_name, "lcm2000nrfav2021" ,\
        "Urban", nrfa_urban, "automatic", "", "Urban", \
        "proportion", "171+172" ])

        output_file.loc[index_no+9] = ([station_name, "lcm2000nrfav2021" , \
        "Unknown", nrfa_unknown, "automatic", "", "Uknown", \
        "proportion", "9999" ])
                
        now = int(time.time())

        output_file.to_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/LCM2000_{0}_{1}.csv".format(station_name, now))'''
