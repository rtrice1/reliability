"""
Settings for the Extract and Transform Module of the FVL MFOP analysis module.
"""

from distutils.sysconfig import PREFIX
import pyodbc
import pandas as pd
from datetime import datetime
from scipy.stats import weibull_min
from scipy.stats import power_divergence


# The most important setting of them all
DEBUG = False
PREFIX = "AH64E"   ##  Prefix for output files in case you this again... make sure you change this (or the output directory!)
# Data Source Definition(s)
DSTYPE = 'File'   #Valid values are File or Database
DATAFILE = 'AH64E_SCORED_2013toPres.csv'
DATADIR = 'Data/'
CONN = ""
# CONN = pyodbc.connect(driver='ODBC Driver 17 for SQL Server',
#                        server='SE-vSQL2',
#                        database='RAM',
#                        trusted_connection='yes')
#Note RELEVANT_BEG_AGE is here 2x
ALL_DATA_SQL = """"
    select 
    KEY13, EI_SN, EVENT_DATE_TIME, RELEVANT_BEG_AGE,     
    RELEVANT_BEG_AGE, MAL_EFF, CORR_DATE_TIME, 
    EI_CORR_AGE, TMMH, TMEN, TIMH, in_phase, in_qc, 
    RFG, SCD1, SCD2, SCD3, SCD4, SCD5, SCD6, SCD7, SCD8, SCD9, PRIMARY_EVENT
    from 
    RAM.dbo.Master_Short_vw_AH64E 
    where 
    EVENT_DATE_TIME > '2013'
    order by 
    KEY13
    """
# Threshold DateTime and Hour diff for gap determination
MINGAPSIZE_DAYS = 30
MINGAPSIZE_HOURS = 10
# Where to save the files and in what formats (I like pkl, but other types are faster/better), I also include CSV just so I can look it it in excel but CSV slows down everything tremendously
OUTPUT_DIR = "Output/" 
OUTPUT_TYPES = ['csv','pkl']




