'''Script to merge all FEH descriptors and create null values, ready to be 
uploaded to Oracle'''


import os
import sys
import numpy as np
import pandas as pd
import time
import glob

'''Merges csv files located in the same folder'''
def combine_csv(folder):
    os.chdir(folder)
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    #export to csv
    combined_csv.to_csv( "combined.csv", index=False, encoding='utf-8-sig')

def make_changes_in_csv(file):
    df = pd.read_csv(file)

    #Select all CCAR and make them sq.km.
    df.loc[df['PROPERTY_ITEM'] == 'CCAR', 'PROPERTY_VALUE'] = df['PROPERTY_VALUE']/400

    #Make nulls
    df.loc[(df['PROPERTY_ITEM'] == 'QUC2') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = "" 
    df.loc[(df['PROPERTY_ITEM'] == 'QUL2') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = "" 
    df.loc[(df['PROPERTY_ITEM'] == 'QUCO') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QUCO') & (df['PROPERTY_VALUE'] == -2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QULO') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    #df.loc[(df['PROPERTY_ITEM'] == 'RM_C') & (df['PROPERTY_VALUE'] == -0.02), 'PROPERTY_VALUE'] = ""
    #df.loc[(df['PROPERTY_ITEM'] == 'RM_C') & (df['PROPERTY_VALUE'] == -.02), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'RM_C') & (df['PROPERTY_VALUE'] == 1), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QUCO') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QUCO') & (df['PROPERTY_VALUE'] == -.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QS69') & (df['PROPERTY_VALUE'] == -2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QS47') & (df['PROPERTY_VALUE'] == -2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QASB') & (df['PROPERTY_VALUE'] == -2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QDPS') & (df['PROPERTY_VALUE'] == -0.2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QR1D') & (df['PROPERTY_VALUE'] == -0.2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QR1H') & (df['PROPERTY_VALUE'] == -0.2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QR2D') & (df['PROPERTY_VALUE'] == -0.2), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QFPL') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QFPX') & (df['PROPERTY_VALUE'] == -0.0002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'Q_D1') & (df['PROPERTY_VALUE'] == -0.00002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'Q_D2') & (df['PROPERTY_VALUE'] == -0.00002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'Q_D3') & (df['PROPERTY_VALUE'] == -0.00002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'Q__E') & (df['PROPERTY_VALUE'] == -0.00002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'Q__C') & (df['PROPERTY_VALUE'] == 0.00002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QFAR') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QBFI') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QASV') & (df['PROPERTY_VALUE'] == -0.02), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QFPD') & (df['PROPERTY_VALUE'] == -0.002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QDPB') & (df['PROPERTY_VALUE'] == -0.02), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QPRW') & (df['PROPERTY_VALUE'] == -0.02), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QUE2') & (df['PROPERTY_VALUE'] == -0.0002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QUEX') & (df['PROPERTY_VALUE'] == -0.0002), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QSPR') & (df['PROPERTY_VALUE'] == -0.02), 'PROPERTY_VALUE'] = ""
    df.loc[(df['PROPERTY_ITEM'] == 'QALT') & (df['PROPERTY_VALUE'] == -2), 'PROPERTY_VALUE'] = ""


    
    #Change various LCM2015 categories from codes to words
    df.loc[(df['PROPERTY_ITEM'] == '2015_1'), 'PROPERTY_ITEM'] = "Broadleaved Woodland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_2'), 'PROPERTY_ITEM'] = "Coniferous Woodland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_3'), 'PROPERTY_ITEM'] = "Arable and Horticulture"
    df.loc[(df['PROPERTY_ITEM'] == '2015_4'), 'PROPERTY_ITEM'] = "Improved Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_5'), 'PROPERTY_ITEM'] = "Neutral Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_6'), 'PROPERTY_ITEM'] = "Calcareous Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_7'), 'PROPERTY_ITEM'] = "Acid Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_8'), 'PROPERTY_ITEM'] = "Fen, Marsh and Swamp"
    df.loc[(df['PROPERTY_ITEM'] == '2015_9'), 'PROPERTY_ITEM'] = "Heather"
    df.loc[(df['PROPERTY_ITEM'] == '2015_10'), 'PROPERTY_ITEM'] = "Heather Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2015_11'), 'PROPERTY_ITEM'] = "Bog"
    df.loc[(df['PROPERTY_ITEM'] == '2015_12'), 'PROPERTY_ITEM'] = "Inland Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2015_13'), 'PROPERTY_ITEM'] = "Saltwater"
    df.loc[(df['PROPERTY_ITEM'] == '2015_14'), 'PROPERTY_ITEM'] = "Freshwater"
    df.loc[(df['PROPERTY_ITEM'] == '2015_15'), 'PROPERTY_ITEM'] = "Supra-littoral Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2015_16'), 'PROPERTY_ITEM'] = "Supra-littoral Sediment"
    df.loc[(df['PROPERTY_ITEM'] == '2015_17'), 'PROPERTY_ITEM'] = "Littoral Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2015_18'), 'PROPERTY_ITEM'] = "Littoral sediment"
    df.loc[(df['PROPERTY_ITEM'] == '2015_19'), 'PROPERTY_ITEM'] = "Saltmarsh"
    df.loc[(df['PROPERTY_ITEM'] == '2015_20'), 'PROPERTY_ITEM'] = "Urban"
    df.loc[(df['PROPERTY_ITEM'] == '2015_21'), 'PROPERTY_ITEM'] = "Suburban"
    df.loc[(df['PROPERTY_ITEM'] == '2015_9999'), 'PROPERTY_ITEM'] = "Unknown"
    df['TITLE'] = df['PROPERTY_ITEM']

    #Change various LCM2007 categories from codes to words
    df.loc[(df['PROPERTY_ITEM'] == '2007_1'), 'PROPERTY_ITEM'] = "Broadleaved Woodland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_2'), 'PROPERTY_ITEM'] = "Coniferous Woodland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_3'), 'PROPERTY_ITEM'] = "Arable and Horticulture"
    df.loc[(df['PROPERTY_ITEM'] == '2007_4'), 'PROPERTY_ITEM'] = "Improved Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_5'), 'PROPERTY_ITEM'] = "Rough/Unmanaged Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_6'), 'PROPERTY_ITEM'] = "Neutral Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_7'), 'PROPERTY_ITEM'] = "Calcareous Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_8'), 'PROPERTY_ITEM'] = "Acid Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_9'), 'PROPERTY_ITEM'] = "Fen, Marsh and Swamp"
    df.loc[(df['PROPERTY_ITEM'] == '2007_10'), 'PROPERTY_ITEM'] = "Heather"
    df.loc[(df['PROPERTY_ITEM'] == '2007_11'), 'PROPERTY_ITEM'] = "Heather Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2007_12'), 'PROPERTY_ITEM'] = "Bog"
    df.loc[(df['PROPERTY_ITEM'] == '2007_13'), 'PROPERTY_ITEM'] = "Montane Habitats"
    df.loc[(df['PROPERTY_ITEM'] == '2007_14'), 'PROPERTY_ITEM'] = "Inland Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2007_15'), 'PROPERTY_ITEM'] = "Saltwater"
    df.loc[(df['PROPERTY_ITEM'] == '2007_16'), 'PROPERTY_ITEM'] = "Freshwater"
    df.loc[(df['PROPERTY_ITEM'] == '2007_17'), 'PROPERTY_ITEM'] = "Supra-littoral Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2007_18'), 'PROPERTY_ITEM'] = "Supra-littoral Sediment"
    df.loc[(df['PROPERTY_ITEM'] == '2007_19'), 'PROPERTY_ITEM'] = "Littoral Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2007_20'), 'PROPERTY_ITEM'] = "Littoral sediment"
    df.loc[(df['PROPERTY_ITEM'] == '2007_21'), 'PROPERTY_ITEM'] = "Saltmarsh"
    df.loc[(df['PROPERTY_ITEM'] == '2007_22'), 'PROPERTY_ITEM'] = "Urban"
    df.loc[(df['PROPERTY_ITEM'] == '2007_23'), 'PROPERTY_ITEM'] = "Suburban"
    df.loc[(df['PROPERTY_ITEM'] == '2007_9999'), 'PROPERTY_ITEM'] = "Unknown"
    df['TITLE'] = df['PROPERTY_ITEM']

    #Change various LCM2000 categories from codes to words
    df.loc[(df['PROPERTY_ITEM'] == '2000_11'), 'PROPERTY_ITEM'] = "Broad Woodland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_21'), 'PROPERTY_ITEM'] = "Coniferous Woodland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_41'), 'PROPERTY_ITEM'] = "Arable Cereals"
    df.loc[(df['PROPERTY_ITEM'] == '2000_42'), 'PROPERTY_ITEM'] = "Arable Horticulture"
    df.loc[(df['PROPERTY_ITEM'] == '2000_43'), 'PROPERTY_ITEM'] = "Non Rotational Horticulture"
    df.loc[(df['PROPERTY_ITEM'] == '2000_51'), 'PROPERTY_ITEM'] = "Improved Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_52'), 'PROPERTY_ITEM'] = "Setaside Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_61'), 'PROPERTY_ITEM'] = "Neutral Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_71'), 'PROPERTY_ITEM'] = "Calcareous Grassaland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_81'), 'PROPERTY_ITEM'] = "Acid Grassland"
    df.loc[(df['PROPERTY_ITEM'] == '2000_91'), 'PROPERTY_ITEM'] = "Bracken"
    df.loc[(df['PROPERTY_ITEM'] == '2000_111'), 'PROPERTY_ITEM'] = "Marsh/Swamp"
    df.loc[(df['PROPERTY_ITEM'] == '2000_101'), 'PROPERTY_ITEM'] = "Dense Shrub"
    df.loc[(df['PROPERTY_ITEM'] == '2000_102'), 'PROPERTY_ITEM'] = "Open Shrub per"
    df.loc[(df['PROPERTY_ITEM'] == '2000_121'), 'PROPERTY_ITEM'] = "Bog"
    df.loc[(df['PROPERTY_ITEM'] == '2000_151'), 'PROPERTY_ITEM'] = "Montane Habitat"
    df.loc[(df['PROPERTY_ITEM'] == '2000_131'), 'PROPERTY_ITEM'] = "Inland Water"
    df.loc[(df['PROPERTY_ITEM'] == '2000_221'), 'PROPERTY_ITEM'] = "Estuary"
    df.loc[(df['PROPERTY_ITEM'] == '2000_181'), 'PROPERTY_ITEM'] = "Supra Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2000_191'), 'PROPERTY_ITEM'] = "Supra Sediment"
    df.loc[(df['PROPERTY_ITEM'] == '2000_201'), 'PROPERTY_ITEM'] = "Littoral Rock"
    df.loc[(df['PROPERTY_ITEM'] == '2000_211'), 'PROPERTY_ITEM'] = "Littoral Sediment"
    df.loc[(df['PROPERTY_ITEM'] == '2000_212'), 'PROPERTY_ITEM'] = "Saltmarsh"
    df.loc[(df['PROPERTY_ITEM'] == '2000_161'), 'PROPERTY_ITEM'] = "Bareground"
    df.loc[(df['PROPERTY_ITEM'] == '2000_171'), 'PROPERTY_ITEM'] = "Suburban"
    df.loc[(df['PROPERTY_ITEM'] == '2000_172'), 'PROPERTY_ITEM'] = "Urban"
    df.loc[(df['PROPERTY_ITEM'] == '2000_9999'), 'PROPERTY_ITEM'] = "Unknown"
    df['TITLE'] = df['PROPERTY_ITEM']

    #Write the output as new file
    df.to_csv("upload_to_Oracle.csv")

if __name__ == "__main__":
    
    
    #Change the root folder
    combine_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/")

    #Change 
    make_changes_in_csv("/prj/nrfa_dev/nrfa_grids_cp/automate_nrfa/TESTING_WITH_ORACLE/CSV/combined.csv")

