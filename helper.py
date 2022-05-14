import pyodbc
import pandas as pd
from datetime import datetime
from scipy.stats import weibull_min
from scipy.stats import power_divergence
from settings import OUTPUT_DIR,OUTPUT_TYPES
import traceback
import re

def sql_data():
    from settings import CONN, ALL_DATA_SQL
    cursor = CONN.cursor()
    return pd.read_sql(ALL_DATA_SQL, CONN)



def shorten_rfg(rfg):  #Assuming RFGs all follow the standard 2 digits - Letter -  2 Digits - Letter
    if (len(rfg) <=2 ): #Do not reduce if we're already 2 digits
        return rfg
    if (rfg[-1].isalpha()) :
        #print("Shortening " + rfg + " -> " + rfg[:-1])  #Reduce by 1 character if the last char is a letter
        return rfg[:-1]
    else :
        #print("Shortening " + rfg + " -> " + rfg[:-2])  #Reduce by 2 if the last char is a number
        return rfg[:-2]

def save_df(df):
    stack = traceback.extract_stack()
    filename, lineno, function_name, code = stack[-2]
    vars_name = re.compile(r'\((.*?)\).*$').search(code).groups()[0]
    for typ in OUTPUT_TYPES:
        if (typ == 'csv') : 
            df.to_csv(OUTPUT_DIR + vars_name + '.csv')
        if (typ == 'pkl') :
            df.to_pickle(OUTPUT_DIR + vars_name +'.pkl')


# ####
# def shorten_rfg(rfg):  #Assuming RFGs all follow the standard 2 digits - Letter -  2 Digits - Letter
#     if (len(rfg) <=2 ): #Do not reduce if we're already 2 digits
#         return rfg
#     if (rfg[-1].isalpha()) :
#         #print("Shortening " + rfg + " -> " + rfg[:-1])  #Reduce by 1 character if the last char is a letter
#         return rfg[:-1]
#     else :
#         #print("Shortening " + rfg + " -> " + rfg[:-2])  #Reduce by 2 if the last char is a number
#         return rfg[:-2]