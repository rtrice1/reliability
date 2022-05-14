# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 16:00:52 2019

@author: AleithaweJI
"""

#import pyodbc
import pandas as pd
from datetime import datetime
from scipy.stats import weibull_min
from scipy.stats import power_divergence

# conn1 = pyodbc.connect(driver='ODBC Driver 17 for SQL Server',
#                       server='SE-vSQL2',
#                       database='RAM',
#                       trusted_connection='yes')
# cursor = conn1.cursor()
# sql_GAP = """
# select 
# KEY13, EI_SN, EVENT_DATE_TIME, RELEVANT_BEG_AGE, 
# RELEVANT_BEG_AGE, MAL_EFF, CORR_DATE_TIME, 
# EI_CORR_AGE, TMMH, TMEN, TIMH, in_phase, in_qc, 
# RFG, SCD1, SCD2, SCD3, SCD4, SCD5, SCD6, SCD7, SCD8, SCD9, PRIMARY_EVENT
# from 
# RAM.dbo.Master_Short_vw_AH64E 
# where 
# EVENT_DATE_TIME > '2013'
# order by 
# KEY13
# """
# AH64E_SCORED_2013toPres = pd.read_sql(sql_GAP, conn1)
# sql = """
# select 
# KEY13, EI_SN, EVENT_DATE_TIME, RELEVANT_BEG_AGE, 
# RELEVANT_BEG_AGE, MAL_EFF, CORR_DATE_TIME, 
# EI_CORR_AGE, TMMH, TMEN, TIMH, in_phase, in_qc, 
# RFG, SCD1, SCD2, SCD3, SCD4, SCD5, SCD6, SCD7, SCD8, SCD9, PRIMARY_EVENT
# from 
# RAM.dbo.Master_Short_vw_AH64E 
# where 
# EVENT_DATE_TIME > '2013'
# and SCD2 != 'X'
# and SCD3 != 'N'
# and SCD5 != 'N'
# order by 
# KEY13
# """
# AH64E_2013toPres_SCORED = pd.read_sql(sql, conn1)
# sqlList = """
# select distinct EI_SN
# from 
# RAM.dbo.Master_Short_vw_AH64E 
# where 
# EVENT_DATE_TIME > '2013'
# and SCD2 != 'X'
# and SCD3 != 'N'
# and SCD5 != 'N'
# order by 
# EI_SN
# """
# AH64E_TailNumList = pd.read_sql(sqlList, conn1)
# sqlList = """
# select distinct RFG
# from 
# RAM.dbo.Master_Short_vw_AH64E 
# where 
# EVENT_DATE_TIME > '2013'
# and SCD2 != 'X'
# and SCD3 != 'N'
# and SCD5 != 'N'
# order by 
# RFG
# """
# AH64E_RFGList = pd.read_sql(sqlList, conn1)
# sqlList = """
# select distinct SCD1
# from 
# RAM.dbo.Master_Short_vw_AH64E 
# where 
# EVENT_DATE_TIME > '2013'
# and SCD2 != 'X'
# and SCD3 != 'N'
# and SCD5 != 'N'
# order by 
# SCD1
# """
# AH64E_SCD1List = pd.read_sql(sqlList, conn1)
# sqlList = """
# select system_id, rfg
#   from RAM.dbo.System_RFGs
#   where system_id = 'AH-64E'
# """
# AH64E_SystemRFGs = pd.read_sql(sqlList, conn1)
# conn1.close()


##############################################################################
###################################       ####################################
############################   Section: "Gaps"   #############################
###################################       ####################################
############# Gather the data that shows the gaps in scored data #############
##############################################################################

minGapSize_Days = 30
minGapSize_Hours = 10

AH64E_SCORED_2013toPres = pd.read_csv('AH64E_SCORED_2013toPres.csv')  #Read all the data
AH64E_2013toPres_SCORED = AH64E_SCORED_2013toPres[
                            (AH64E_SCORED_2013toPres['SCD2'] != 'X') & 
                            (AH64E_SCORED_2013toPres['SCD3'] != 'N') & 
                            (AH64E_SCORED_2013toPres['SCD5'] != 'N')
                        ]
AH64E_TailNumList = AH64E_SCORED_2013toPres['EI_SN'].unique()        #grab 
RFGList     = AH64E_SCORED_2013toPres['RFG'].unique()
AH64E_RFGList = pd.DataFrame(data = RFGList, columns = ['RFG'])
SCD1List    = AH64E_SCORED_2013toPres['SCD1'].unique()
AH64E_SCD1List = pd.DataFrame(data = SCD1List, columns = ['SCD1'])
SystemRFGs  = AH64E_RFGList.copy()
AH64E_SystemRFGs = pd.DataFrame(data = SystemRFGs, columns = ['rfg'])
# Write it to CSV
#AH64E_SCORED_2013toPres.to_csv('AH64E_SCORED_2013toPres.csv')  (don't write to CSV)

# Delete columns that aren't useful for gap analysis
AH64E_SCORED_2013toPres = AH64E_SCORED_2013toPres.drop(columns = ['MAL_EFF', 'CORR_DATE_TIME', 'EI_CORR_AGE', 
                                                                'TMMH', 'TMEN', 'TIMH', 'in_phase', 'in_qc', 
                                                                'RFG', 'SCD1', 'SCD2', 'SCD3', 'SCD4', 'SCD5', 
                                                                'SCD6', 'SCD7', 'SCD8', 'SCD9', 'PRIMARY_EVENT'])

AH64E_SCORED_2013toPres = AH64E_SCORED_2013toPres.iloc[:,:-1] # delete the last column of the df (this is the second "RELEVANT_BEG_AGE" column)

# Make lists of tail unique numbers
# Adjust for pulling from CSV
try:
   AH64E_SCORED_2013toPres = AH64E_SCORED_2013toPres.drop(columns = ['Unnamed: 0'])  # store as pkl and remove extra RELEVANT_BEG_AGE from sql
except:
    print("Superfluous columns removed already")


print (AH64E_SCORED_2013toPres.head())
TailNumbers_SCORED = AH64E_SCORED_2013toPres.iloc[:,1]
TailNumbers_SCORED = TailNumbers_SCORED.to_frame()
print(TailNumbers_SCORED.head())
TailNumbers_SCORED = TailNumbers_SCORED.drop_duplicates("EI_SN", keep = 'first')
TailNumbers_SCORED = TailNumbers_SCORED.reset_index(drop=True)

# Threshold DateTime and Hour diff for gap determination
minGapSize_Days = 30
minGapSize_Hours = 10
# Get range of Date-Time without gaps for all data, scored data, and unscored data
# This needs to be done per tail number
# Loop for finding data ranges without gaps
columnNames = ['TailNumber', 'StartDtTm', 'EndDtTm'] # Just for reference while updating to new columns#####################################################################################################################
NEWcolumnNames = ['TailNumber', 'GapCount', 'Start1', 'End1', 'Start2', 'End2', 'Start3', 'End3', 'Start4', 'End4']



strStatus = 'Scored'
DataPlaceHolder = AH64E_SCORED_2013toPres
TailNumbers = TailNumbers_SCORED
PlaceHolderDtTm = pd.DataFrame(columns = columnNames)
list_TailNumbers = DataPlaceHolder['EI_SN'].tolist()
list_DateTime = DataPlaceHolder['EVENT_DATE_TIME'].tolist()
list_Hours = DataPlaceHolder['RELEVANT_BEG_AGE'].tolist()

# Begin loop to recored date ranges
StartLoopIndex = 1
for o in range(len(TailNumbers)):
    curTailNumber = TailNumbers.iloc[o,0]
    GapFound = 1 # to identify whether or not a gap was found on last iteration
    print(strStatus, '--', (o + 1), '/', len(TailNumbers), '--', datetime.now())
    for p in range(StartLoopIndex, len(DataPlaceHolder)):
        # Pandas does something weird sometimes to the date formatting
        # This is just a work-around
        if len(str(list_DateTime[(p-1)])) == 23:
            StartDtTm = datetime.strptime(str(list_DateTime[(p-1)]), '%Y-%m-%d %H:%M:%S.%f')
        elif len(str(list_DateTime[(p-1)])) == 19:
            StartDtTm = datetime.strptime(str(list_DateTime[(p-1)]), '%Y-%m-%d %H:%M:%S')
        if len(str(list_DateTime[(p)])) == 23:
            EndDtTm = datetime.strptime(str(list_DateTime[(p)]), '%Y-%m-%d %H:%M:%S.%f')
        elif len(str(list_DateTime[(p)])) == 19:
            EndDtTm = datetime.strptime(str(list_DateTime[(p)]), '%Y-%m-%d %H:%M:%S')
        
        StartHours = list_Hours[(p-1)]
        EndHours = list_Hours[p]
        DiffDtTm = (EndDtTm - StartDtTm).days
        DiffHours = (EndHours - StartHours)
        
        if list_TailNumbers[(p)] == curTailNumber and list_TailNumbers[(p-1)] == curTailNumber:
            if DiffDtTm < minGapSize_Days and DiffHours < minGapSize_Hours and GapFound == 1:
                GapFound = 0
                PlaceHolderDtTm.loc[len(PlaceHolderDtTm)] = curTailNumber
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 1] = StartDtTm
                # This is just to fix an issue recording the last TailNumber's final entry
                if o == (len(TailNumbers) - 1) and p == (len(DataPlaceHolder) - 1):
                    PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = EndDtTm
            elif DiffDtTm >= minGapSize_Days and DiffHours >= minGapSize_Hours and GapFound == 0:
                GapFound = 1
                # Save using StartDtTm because EndDtTm represents the first value on the other side of a gap
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = StartDtTm
                # This is just to fix an issue recording the last TailNumber's final entry
                if o == (len(TailNumbers) - 1) and p == (len(DataPlaceHolder) - 1):
                    PlaceHolderDtTm.loc[len(PlaceHolderDtTm)] = curTailNumber
                    PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 1] = EndDtTm
                    PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = EndDtTm
            elif DiffDtTm >= minGapSize_Days and DiffHours >= minGapSize_Hours and GapFound == 1:
                PlaceHolderDtTm.loc[len(PlaceHolderDtTm)] = curTailNumber
                # Save using StartDtTm because EndDtTm represents the first value on the other side of a gap
                # This is showing a single data point in between gaps
                # So, it is recorded as the same StartDtTm and EndDtTm
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 1] = StartDtTm
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = StartDtTm
                # This is just to fix an issue recording the last TailNumber's final entry
                if o == (len(TailNumbers) - 1) and p == (len(DataPlaceHolder) - 1):
                    PlaceHolderDtTm.loc[len(PlaceHolderDtTm)] = curTailNumber
                    PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 1] = EndDtTm
                    PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = EndDtTm
            elif DiffDtTm < minGapSize_Days and DiffHours < minGapSize_Hours and GapFound == 0 and o == (len(TailNumbers) - 1) and p == (len(DataPlaceHolder) - 1):
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = EndDtTm
        # This is for the last entry for a particular tail number or if a tail number has only one entry
        elif list_TailNumbers[(p)] != curTailNumber and list_TailNumbers[(p-1)] == curTailNumber:
            if GapFound == 1:
                PlaceHolderDtTm.loc[len(PlaceHolderDtTm)] = curTailNumber
                # Save using StartDtTm because EndDtTm represents the first value on the other side of a gap
                # This is showing a single data point in between gaps
                # So, it is recorded as the same StartDtTm and EndDtTm
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 1] = StartDtTm
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = StartDtTm
            elif GapFound == 0:
                PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = StartDtTm
            StartLoopIndex = p
            break
        # This is just to fix an issue recording the last TailNumber's final entry
        if list_TailNumbers[(p)] == curTailNumber and list_TailNumbers[(p-1)] != curTailNumber and o == (len(TailNumbers) - 1) and p == (len(DataPlaceHolder) - 1):
            PlaceHolderDtTm.loc[len(PlaceHolderDtTm)] = list_TailNumbers[(p)]
            PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 1] = EndDtTm
            PlaceHolderDtTm.iloc[(len(PlaceHolderDtTm)-1), 2] = EndDtTm

AH64E_ScoredDtTm = PlaceHolderDtTm
del(strStatus)
del(DataPlaceHolder)
del(TailNumbers)
del(PlaceHolderDtTm)
        
# Write the gap dataframes to CSVs
#AH64E_ScoredDtTm.to_csv('AH64E_ScoredDtTm.csv')

# Counts of each tail number. This will show which ones have gaps
# Gaps need two datapoints on a tail number
GapCounts = AH64E_ScoredDtTm.groupby(['TailNumber']).size().reset_index().rename(columns={0:'DataPoints'})

# Delete TailNumbers that dont have a gap
GapCounts = GapCounts.sort_values(['DataPoints'])
GapCounts = GapCounts.reset_index(drop=True)
idxDELETE = (GapCounts.DataPoints.values > 1).argmax()
DeleteLines = len(GapCounts) - idxDELETE
GapCounts = GapCounts[:-DeleteLines]

# Get list of index of Tail Numbers that dont have gaps and delete them
DeleteIndexes = []
for n in range(len(GapCounts)):
    curTailNumber = GapCounts.iloc[n, 0]
    curIndex = AH64E_ScoredDtTm[(AH64E_ScoredDtTm.TailNumber == curTailNumber)].index.tolist()
    DeleteIndexes.extend(curIndex)
AH64E_ScoredDtTm_GAPS = AH64E_ScoredDtTm.drop(AH64E_ScoredDtTm.index[DeleteIndexes])
AH64E_ScoredDtTm_GAPS = AH64E_ScoredDtTm_GAPS.sort_values(['TailNumber', 'StartDtTm'])
AH64E_ScoredDtTm_GAPS = AH64E_ScoredDtTm_GAPS.reset_index(drop=True)

# Get the gap counts of the new dataframe
GapCounts = AH64E_ScoredDtTm_GAPS.groupby(['TailNumber']).size().reset_index().rename(columns={0:'DataPoints'})
AH64E_ScoredDtTm_GAPS.to_csv('AH64E_ScoredDtTm_GAPS.csv')
if GapCounts.DataPoints.nunique() == 1 and GapCounts.iloc[2,1] == 2:
    lstTNs = GapCounts['TailNumber'].tolist()
    lstGapStart = []
    lstGapEnd = []
    for a in range(int(len(AH64E_ScoredDtTm_GAPS)/2)):
        lstGapStart.append(AH64E_ScoredDtTm_GAPS.iloc[a*2,2])
        lstGapEnd.append(AH64E_ScoredDtTm_GAPS.iloc[(a*2)+1,1])
    AH64E_ScoredDtTm_GAPS = pd.DataFrame(list(zip(lstTNs, lstGapStart, lstGapEnd)), columns = ['TailNumber', 'GapStart', 'GapEnd'])
    AH64E_ScoredDtTm_GAPS.to_csv('AH64E_ScoredDtTm_GAPS.csv')

print("Done Find Gaps ", '--', datetime.now()) # printing this string as the status
##############################################################################
###################################       ####################################
###############################   Section 0   ################################
###################################       ####################################
################## Just doing some basic Data PreProcessing ##################
##############################################################################


# Go ahead and save these SQL data pulls to CSVs
#AH64E_2013toPres_SCORED.to_csv('AH64E_2013toPres_SCORED.csv')
#AH64E_TailNumList.to_csv('AH64E_TailNumList.csv')
#AH64E_RFGList.to_csv('AH64E_RFGList.csv')
#AH64E_SCD1List.to_csv('AH64E_SCD1List.csv')

## Read in CSV, just cause I don't want to rerun SQL pull each time
#AH64E_2013toPres_SCORED = pd.read_csv('AH64E_2013toPres_SCORED.csv')
#AH64E_TailNumList = pd.read_csv('AH64E_TailNumList.csv')
#AH64E_RFGList = pd.read_csv('AH64E_RFGList.csv')
#AH64E_SCD1List = pd.read_csv('AH64E_SCD1List.csv')
#AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.drop(columns = ['Unnamed: 0'])
#AH64E_TailNumList = AH64E_TailNumList.drop(columns = ['Unnamed: 0'])
#AH64E_RFGList = AH64E_RFGList.drop(columns = ['Unnamed: 0'])
#AH64E_SCD1List = AH64E_SCD1List.drop(columns = ['Unnamed: 0'])

# Read in CSV, just cause it's easier this way
AH64E_2013toPres_SCORED = pd.read_csv('AH64E_2013toPres_SCORED.csv')
# Rename index column that is created when saved to CSV to be placeholder for new ID column
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.rename(columns={"Unnamed: 0": "Key13 / RFG / EventClass"})

# Create index lists for each of the Event Classifications by the SCD1 rules in SQL
lstEMAs = AH64E_2013toPres_SCORED[((AH64E_2013toPres_SCORED.SCD2 != 'N') & (AH64E_2013toPres_SCORED.SCD2 != 'P') & (AH64E_2013toPres_SCORED.SCD2 != 'X') & (AH64E_2013toPres_SCORED.SCD2 != 'Z') & (AH64E_2013toPres_SCORED.SCD2 != '')) & (AH64E_2013toPres_SCORED.SCD3 == 'C') & (AH64E_2013toPres_SCORED.SCD8 != 'N') & (AH64E_2013toPres_SCORED.RFG != '36B')].index.tolist()
lstMAs = AH64E_2013toPres_SCORED[(AH64E_2013toPres_SCORED.SCD3 == 'C') & (AH64E_2013toPres_SCORED.SCD4 != 'O') & (AH64E_2013toPres_SCORED.SCD4 != 'H') & ((AH64E_2013toPres_SCORED.SCD2 == 'J') | (AH64E_2013toPres_SCORED.SCD2 == 'K') | (AH64E_2013toPres_SCORED.SCD2 == 'C') | (AH64E_2013toPres_SCORED.SCD2 == 'S') | (AH64E_2013toPres_SCORED.SCD2 == 'W') | (AH64E_2013toPres_SCORED.SCD2 == 'Q') | (AH64E_2013toPres_SCORED.SCD2 == 'U')) & ((AH64E_2013toPres_SCORED.SCD5 == '1') | (AH64E_2013toPres_SCORED.SCD5 == '2') | (AH64E_2013toPres_SCORED.SCD5 == '4')) & (AH64E_2013toPres_SCORED.RFG != '36B')].index.tolist()
lstMAFs = AH64E_2013toPres_SCORED[((AH64E_2013toPres_SCORED.SCD2 != 'D') & (AH64E_2013toPres_SCORED.SCD2 != 'N') & (AH64E_2013toPres_SCORED.SCD2 != 'P') & (AH64E_2013toPres_SCORED.SCD2 != 'X') & (AH64E_2013toPres_SCORED.SCD2 != 'Z') & (AH64E_2013toPres_SCORED.SCD2 != '')) & (AH64E_2013toPres_SCORED.SCD3 == 'C') & (AH64E_2013toPres_SCORED.SCD8 != 'N') & (AH64E_2013toPres_SCORED.SCD9 != 'N') & (AH64E_2013toPres_SCORED.RFG != '36B')].index.tolist()
lstSchedMaints = AH64E_2013toPres_SCORED[(AH64E_2013toPres_SCORED.SCD2 != 'X') & (AH64E_2013toPres_SCORED.SCD3 == 'C') & (AH64E_2013toPres_SCORED.SCD5 == 'S') & (AH64E_2013toPres_SCORED.RFG != '36B')].index.tolist()
lstUMAs = AH64E_2013toPres_SCORED[((AH64E_2013toPres_SCORED.SCD2 != 'X') & (AH64E_2013toPres_SCORED.SCD2 != 'Z')) & (AH64E_2013toPres_SCORED.SCD3 == 'C') & ((AH64E_2013toPres_SCORED.SCD5 != 'M') & (AH64E_2013toPres_SCORED.SCD5 != 'R') & (AH64E_2013toPres_SCORED.SCD5 != 'S')) & (AH64E_2013toPres_SCORED.RFG != '36B')].index.tolist()
lstUnschedMaints = AH64E_2013toPres_SCORED[(AH64E_2013toPres_SCORED.SCD5 != 'S') & (AH64E_2013toPres_SCORED.SCD2 != 'X') & (AH64E_2013toPres_SCORED.SCD3 == 'C')].index.tolist()

# Use lists to make dataframes of data specific to only the relative Event Classification
EMA_All_WeibullData = AH64E_2013toPres_SCORED.loc[AH64E_2013toPres_SCORED.index.isin(lstEMAs)]
EMA_All_WeibullData = EMA_All_WeibullData.reset_index(drop=True)
MA_All_WeibullData = AH64E_2013toPres_SCORED.loc[AH64E_2013toPres_SCORED.index.isin(lstMAs)]
MA_All_WeibullData = MA_All_WeibullData.reset_index(drop=True)
MAF_All_WeibullData = AH64E_2013toPres_SCORED.loc[AH64E_2013toPres_SCORED.index.isin(lstMAFs)]
MAF_All_WeibullData = MAF_All_WeibullData.reset_index(drop=True)
SchedMaint_All_WeibullData = AH64E_2013toPres_SCORED.loc[AH64E_2013toPres_SCORED.index.isin(lstSchedMaints)]
SchedMaint_All_WeibullData = SchedMaint_All_WeibullData.reset_index(drop=True)
UMA_All_WeibullData = AH64E_2013toPres_SCORED.loc[AH64E_2013toPres_SCORED.index.isin(lstUMAs)]
UMA_All_WeibullData = UMA_All_WeibullData.reset_index(drop=True)
UnschedMaint_All_WeibullData = AH64E_2013toPres_SCORED.loc[AH64E_2013toPres_SCORED.index.isin(lstUnschedMaints)]
UnschedMaint_All_WeibullData = UnschedMaint_All_WeibullData.reset_index(drop=True)

# Rename SCD1 column to be EventClass
EMA_All_WeibullData = EMA_All_WeibullData.rename(columns={"SCD1": "EventClass"})
MA_All_WeibullData = MA_All_WeibullData.rename(columns={"SCD1": "EventClass"})
MAF_All_WeibullData = MAF_All_WeibullData.rename(columns={"SCD1": "EventClass"})
SchedMaint_All_WeibullData = SchedMaint_All_WeibullData.rename(columns={"SCD1": "EventClass"})
UMA_All_WeibullData = UMA_All_WeibullData.rename(columns={"SCD1": "EventClass"})
UnschedMaint_All_WeibullData = UnschedMaint_All_WeibullData.rename(columns={"SCD1": "EventClass"})

# Create lists to put into EventClass columns
lstEMAstr = ['EMA'] * len(lstEMAs)
lstMAstr = ['MA'] * len(lstMAs)
lstMAFstr = ['MAF'] * len(lstMAFs)
lstSchedMaintstr = ['SchedMaint'] * len(lstSchedMaints)
lstUMAstr = ['UMA'] * len(lstUMAs)
lstUnschedMaintstr = ['UnschedMaint'] * len(UnschedMaint_All_WeibullData)

# Replace SCD1 data with Event Classification data
EMA_All_WeibullData['EventClass'] = lstEMAstr
MA_All_WeibullData['EventClass'] = lstMAstr
MAF_All_WeibullData['EventClass'] = lstMAFstr
SchedMaint_All_WeibullData['EventClass'] = lstSchedMaintstr
UMA_All_WeibullData['EventClass'] = lstUMAstr
UnschedMaint_All_WeibullData['EventClass'] = lstUnschedMaintstr

# Put all the data back together into one DataFrame
AH64E_2013toPres_SCORED = EMA_All_WeibullData.append(MA_All_WeibullData, ignore_index=True)
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.append(MAF_All_WeibullData, ignore_index=True)
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.append(SchedMaint_All_WeibullData, ignore_index=True)
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.append(UMA_All_WeibullData, ignore_index=True)
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.append(UnschedMaint_All_WeibullData, ignore_index=True)

lstNewID = [] # Initialize the list
# Loop through scored data to grab each RFG and SCD for each line
# Place the RFG and SCD after the TailNumber in the Key13
# To create a new Identifier for sorting the data
for h in range(len(AH64E_2013toPres_SCORED)):
    curRFG = AH64E_2013toPres_SCORED.iloc[h,14]
    curEventClass = AH64E_2013toPres_SCORED.iloc[h,15]
    FrontKey13 = AH64E_2013toPres_SCORED.iloc[h,1][:15]
    BackKey13 = AH64E_2013toPres_SCORED.iloc[h,1][14:]
    NewID = FrontKey13 + curRFG + '-' + curEventClass + BackKey13
    lstNewID.append(NewID[3:])
#    print((h + 1), '/', len(AH64E_2013toPres_SCORED), '--', datetime.now())
AH64E_2013toPres_SCORED['Key13 / RFG / EventClass'] = lstNewID
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.sort_values(['Key13 / RFG / EventClass'])
AH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.reset_index(drop=True)

# Go ahead and save this to the new dataframe
NewIdAH64E_2013toPres_SCORED = AH64E_2013toPres_SCORED.drop(columns = ['MAL_EFF', 'CORR_DATE_TIME', 'EI_CORR_AGE', 'in_phase', 'in_qc', 'SCD2', 'SCD3', 'SCD4', 'SCD5', 'SCD6', 'SCD7', 'SCD8', 'SCD9', 'PRIMARY_EVENT'])
NewIdAH64E_2013toPres_SCORED = NewIdAH64E_2013toPres_SCORED.sort_values(['Key13 / RFG / EventClass'])
NewIdAH64E_2013toPres_SCORED = NewIdAH64E_2013toPres_SCORED.reset_index(drop=True)

# Add column for the length of the RFG (for trimming)
lstRFGlengths = [] # Initialize the list
for g in range(len(NewIdAH64E_2013toPres_SCORED)):
#    print((g + 1), '/', len(NewIdAH64E_2013toPres_SCORED), '--', datetime.now())
    lengthRFG = len(NewIdAH64E_2013toPres_SCORED.iloc[g,9])
    lstRFGlengths.append(lengthRFG)
NewIdAH64E_2013toPres_SCORED['lengthRFG'] = lstRFGlengths

# Create dataframe with all RFG and SCD1 combinations and include counts for number of datapoints and Save to CSV
NewIdAH64E_2013toPres_SCORED = NewIdAH64E_2013toPres_SCORED.rename(columns={'EI_SN':'TailNumber'})
#NewIdAH64E_2013toPres_SCORED.to_csv('AH64E_NewId_2013toPres_SCORED.csv')
TN_RFG_SCD_cnt = NewIdAH64E_2013toPres_SCORED.groupby(['TailNumber', 'RFG', 'lengthRFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'})
TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['lengthRFG'], ascending = False)
TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True)
#TN_RFG_SCD_cnt.to_csv('AH64E_TN_RFG_SCD_cnt.csv')
print("Done Section 0 ", '--', datetime.now()) # printing this string as the status

##############################################################################
###################################       ####################################
###############################   Section 1   ################################
###################################       ####################################
######### Trim RFGs that do not have enough data for the analysis ############
#### Delete any leftovers that still dont have enough data after trimming ####
##############################################################################


# Work through the data that doesnt have at least 2 datapoints and trim RFGs
LoopCount = 1 # Initialize the loop counter
LoopCount1 = 0 # Initialize the loop counter part 2
DoneTrimming1 = 0 # Initialize this flag for when we have finished trimming the TN/RFG/EvCl combinations that have only one datapoint
PrintLess = list(range(0, 10000000, 100)) # use this list so we don't print status on every loop
lstTrimmedInfo = [] # Initialize the list
while len(TN_RFG_SCD_cnt) > 0 and TN_RFG_SCD_cnt.iloc[0,2] > 2: # looping as long as there's data in TN_RFG_SCD_cnt, and the first row's RFG is longer than 2 characters
    prntLENGTH = len(TN_RFG_SCD_cnt) # just saving this to a variable for status print in next couple of lines
    print(LoopCount, '--', prntLENGTH, '--', datetime.now()) # printing this string as the status
    
    if LoopCount != 1: # skip this section of the loop on the first iteration
        # Look at first RFG in sparse data list to try to trim it in the Data
        WhileLoopIdx = 0 # Index the while loop
        LongestLength = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,2] # Save the longest RFG length to a variable, cause we want to trim all of the RFGs that are of this same length
        CurrentLength = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,2] # This is the length of the line we are on in the data, so we can check that we are still trimming the longest RFGs on this loop
        while CurrentLength == LongestLength and TN_RFG_SCD_cnt.iloc[WhileLoopIdx,2] > 2: # as long as we are still looking at the longest length of RFGs and the first row's RFG length is greater than 2 (meaning it has space for trimming)
            lstTrimIndex = [] # Initialize/Reset the list
            curTailNumber = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,0] # save the first row's Tail Number to a variable
            curRFG = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,1] # save the first row's RFG to a variable
            curEventClass = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,3] # save the first row's Event Classification to a variable
            lstTrimmedInfo.append([curTailNumber, curRFG, curEventClass]) # appending to a list to capture RFGs being trimmed
            RFG_DataPoints = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,4] # save the first row's number of datapoints to a variable
            lstTrimIndex = NewIdAH64E_2013toPres_SCORED[(NewIdAH64E_2013toPres_SCORED.TailNumber == curTailNumber) & (NewIdAH64E_2013toPres_SCORED.RFG == curRFG) & (NewIdAH64E_2013toPres_SCORED.EventClass == curEventClass)].index.tolist() # look through data and grab indexes of lines that match what we're looking for
            if len(lstTrimIndex) != RFG_DataPoints: # This is just to make sure there wasn't some mismatch between the number of indexes that were found and the number of datapoints we were supposed to find
                AAAAAA = ('***  ', curRFG, '-', curEventClass, '--', len(lstTrimIndex), ' != ', RFG_DataPoints) # so we just print an error message
                break # and we break the loop
            # loop to decide how many characters to trim off the RFG
            for a in range(1, 15): # 15 is just an arbitrary number that is bigger than we need
                if AH64E_SystemRFGs.rfg.isin([curRFG[:(len(curRFG) - a)]]).any(): # if we trim off 'a' chars, is the trimmed RFG in the RFG list
                    break # done. break the loop
            # Find where this TN/RFG/EventClass combo is in the data and trim the RFG
            for r in range(len(lstTrimIndex)): # now we're gonna loop through the actual data
                curIndex = lstTrimIndex[r] # just saving the current index of where we are in the loop through the list of indeces
                TrimChar = len(curRFG) - a # we want to pull 'a' characters off of the end of the RFG ('a' is determined in the preceding for loop)
                NewIdAH64E_2013toPres_SCORED.iloc[curIndex,9] = NewIdAH64E_2013toPres_SCORED.iloc[curIndex,9][:TrimChar] # perform the trimming of the RFG
                NewIdAH64E_2013toPres_SCORED.iloc[curIndex,11] = NewIdAH64E_2013toPres_SCORED.iloc[curIndex,11] - a # update the Length of RFG column to match the newly trimmed RFG
            WhileLoopIdx = WhileLoopIdx + 1 # Add one to the index for the next loop
            CurrentLength = TN_RFG_SCD_cnt.iloc[WhileLoopIdx,2] # Update this value for the next loop
    elif LoopCount == 1:
        LoopCount = LoopCount + LoopCount1 # do this to account for resetting the loopcount after finishing trimming combos with only 1 datapoint
    
    # Get the new Counts on the combinations of RFGs and EventClass
    TN_RFG_SCD_cnt = NewIdAH64E_2013toPres_SCORED.groupby(['TailNumber', 'RFG', 'lengthRFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'}) # count the number of occurences of each combination in the data
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['lengthRFG'], ascending = False) # sort by the length of the RFG, from largest to smallest
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True) # reset the indexes to be aligned with new sorting
    print("Finished part ")
# |||||| ###################################################################################################################################### |||||| #
# |||||| ######################                  this is where the gap issue analysis is being implemented                  ################### |||||| #
# vvvvvv ###################################################################################################################################### vvvvvv #
# this whole section has to be done in the while loop because we are updating the actual data when RFGs are trimmed, this affects how the gap issues are found
    
    # Get lines from TN_RFG_SCD_cnt and NewIdAH64E_2013toPres_SCORED that are TailNumbers listed in AH64E_ScoredDtTm_GAPS
    GapTails = AH64E_ScoredDtTm_GAPS.TailNumber.tolist() # make a list of tail numbers that have gaps
    lstGAPS_Data = NewIdAH64E_2013toPres_SCORED[(NewIdAH64E_2013toPres_SCORED.TailNumber.isin(GapTails))].index.tolist() # make a list of the indexes from the data that map to tail numbers with gaps
    lstGAPS_Params = TN_RFG_SCD_cnt[(TN_RFG_SCD_cnt.TailNumber.isin(GapTails))].index.tolist() # make a list of the indexes from the counts that map to tail numbers with gaps
    Gaps_WeibullData = NewIdAH64E_2013toPres_SCORED.iloc[lstGAPS_Data] # save all the data where there is a gap
    Gaps_TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.iloc[lstGAPS_Params] # save all the counts where there is a gap
    Gaps_WeibullData = Gaps_WeibullData.sort_values(['TailNumber', 'RFG', 'EventClass', 'KEY13']) # resort the data
    Gaps_TN_RFG_SCD_cnt = Gaps_TN_RFG_SCD_cnt.sort_values(['DataPoints']) # resort the counts by the number of datapoints
    Gaps_WeibullData = Gaps_WeibullData.reset_index(drop=True) # reset the indexes after new sorting
    Gaps_TN_RFG_SCD_cnt = Gaps_TN_RFG_SCD_cnt.reset_index(drop=True) # reset the indexes after new sorting
    
    # Add 2 columns to Gaps_TN_RFG_SCD_cnt to record Date-Range for data from NewIdAH64E_2013toPres_SCORED
    lstGapStartDtTm = [] # initialize the list
    lstGapEndDtTm = [] # initialize the list
    for c in range(len(Gaps_TN_RFG_SCD_cnt)): # gonna loop through the counts for the gaps
        curTailNumber = Gaps_TN_RFG_SCD_cnt.iloc[c,0] # save the current tail number to a variable
        curRFG = Gaps_TN_RFG_SCD_cnt.iloc[c,1] # save the current RFG to a variable
        curEventClass = Gaps_TN_RFG_SCD_cnt.iloc[c,3] # save the current event classification to a variable
        lstCurrentIndexes = Gaps_WeibullData[(Gaps_WeibullData.TailNumber == curTailNumber) & (Gaps_WeibullData.RFG == curRFG) & (Gaps_WeibullData.EventClass == curEventClass)].index.tolist() # look through data and grab indexes of lines that match what we're looking for
        lstGapStartDtTm.append(Gaps_WeibullData.iloc[lstCurrentIndexes[0],3]) # grab the date-time from the data for the first index of the lines that matched the combination of current variables and save it to the START list
        lstGapEndDtTm.append(Gaps_WeibullData.iloc[lstCurrentIndexes[len(lstCurrentIndexes)-1],3]) # grab the date-time from the data for the last index of the lines that matched the combination of current variables and save it to the END list
    
    Gaps_TN_RFG_SCD_cnt['DataStart'] = lstGapStartDtTm # add the START list as a new column
    Gaps_TN_RFG_SCD_cnt['DataEnd'] = lstGapEndDtTm # add the END list as a new column
    
    # Check to see if there is common time between the Gap's date-range and the Data's date-range
    lstGapIssue = [] # initialize the list
    for d in range(len(Gaps_TN_RFG_SCD_cnt)): # gonna loop through the counts for the gaps again
        curTailNumber = Gaps_TN_RFG_SCD_cnt.iloc[d,0] # save the current tail number to a variable
        curTNindex = AH64E_ScoredDtTm_GAPS[(AH64E_ScoredDtTm_GAPS.TailNumber == curTailNumber)].index.tolist() # look through AH64E_ScoredDtTm_GAPS and grab indexes of lines that match what we're looking for
        if len(curTNindex) == 1: # check to see that we actually found the current TN in the list of TNs that have gaps
            # When there's a data gap, there will not be any datapoints within the gap range
            # The only scenario where the gap is an issue is when the Beginning Date-Time is before the Gap and the Ending Date-Time is after the Gap
            if AH64E_ScoredDtTm_GAPS.iloc[curTNindex[0],1] >= Gaps_TN_RFG_SCD_cnt.iloc[d,5] and AH64E_ScoredDtTm_GAPS.iloc[curTNindex[0],2] <= Gaps_TN_RFG_SCD_cnt.iloc[d,6]: # check to see if the gap resides in the actual data's date range
                lstGapIssue.append(1) # if it does, flag it
            else:
                lstGapIssue.append(0) # if not, don't flag it
        else:
            lstGapIssue.append(0) # if not, don't flag it
    
    # Let's just look at the ones where the gap is an issue
    Gaps_TN_RFG_SCD_cnt['GapIssue'] = lstGapIssue # take the list of flags and add it as a column to the counts df
    lstDataGapIssues = Gaps_TN_RFG_SCD_cnt[(Gaps_TN_RFG_SCD_cnt.GapIssue == 1)].index.tolist() # get the indexes of the ones that have been flagged as having a gap-issue
    GapIssues_TN_RFG_SCD_cnt = Gaps_TN_RFG_SCD_cnt.iloc[lstDataGapIssues] # use the indexes to make a DF for just the ones with a gap issue
    GapIssues_TN_RFG_SCD_cnt = GapIssues_TN_RFG_SCD_cnt.sort_values(['DataPoints']) # re-sort
    GapIssues_TN_RFG_SCD_cnt = GapIssues_TN_RFG_SCD_cnt.reset_index(drop=True) # re-index
    
    # Need to use the flags on the dataframe we are using for trimming, subtract 1 from the datapoints that lose an interval because of the gap
    for e in range(len(GapIssues_TN_RFG_SCD_cnt)): # gonna loop through the ones that need will lose a datapoint because of the gap
        curTailNumber = GapIssues_TN_RFG_SCD_cnt.iloc[e,0] # save the current tail number to a variable
        curRFG = GapIssues_TN_RFG_SCD_cnt.iloc[e,1] # save the current RFG to a variable
        curEventClass = GapIssues_TN_RFG_SCD_cnt.iloc[e,3] # save the current event classification to a variable
        lstCurrentIndex = TN_RFG_SCD_cnt[(TN_RFG_SCD_cnt.TailNumber == curTailNumber) & (TN_RFG_SCD_cnt.RFG == curRFG) & (TN_RFG_SCD_cnt.EventClass == curEventClass)].index.tolist() # look through data and grab the index of the line that matches what we're looking for
        TN_RFG_SCD_cnt.iloc[lstCurrentIndex[0],4] = TN_RFG_SCD_cnt.iloc[lstCurrentIndex[0],4] - 1 # subtract 1 from the datapoint to account for gap, will need to update this to subtract more than 1 when there is more than 1 gap

# ^^^^^^ ###################################################################################################################################### ^^^^^^ #
# |||||| ######################                  this is where the gap issue analysis is being implemented                  ################### |||||| #
# |||||| ###################################################################################################################################### |||||| #

    # Delete RFGs that have enough data for Weibull ... Re-sort and re-index
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['DataPoints']) # sort by the number of datapoints
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True) # reset the indexes to be aligned with new sorting
    # we want to be left with only what needs to be trimmed
    if DoneTrimming1 == 1: # On the first loops we want to trim only combinations that have 1 datapoint, cause these will not be able to be used if they only have 1 datapoint after trimming (because they cannot create interval data)
        idxDELETE = (TN_RFG_SCD_cnt.DataPoints.values > 5).argmax() # find the index of the first row where there are enough datapoints
    elif DoneTrimming1 == 0:
        idxDELETE = (TN_RFG_SCD_cnt.DataPoints.values > 1).argmax() # find the index of the first row where there are enough datapoints
        if idxDELETE == 0: # if this is true, it means that there are no more with only 1 datapoint
            DoneTrimming1 = 1 # so we are done with this section of trimming, flag it
            LoopCount1 = LoopCount # Save off the loop counter
            LoopCount = 0 # Reset the loop count since we are on the new section of looping, we will add back the loop count each time to keep counting up
            
    # Here we just want to save off the lines that only have one data point -- need this to add to the sparse dataframe right after the while loop completes
    idxDELETE_JustTheOnes = (TN_RFG_SCD_cnt.DataPoints.values > 1).argmax() # find the index of the first row where there are enough datapoints
    DeleteLines_JustTheOnes = len(TN_RFG_SCD_cnt) - idxDELETE_JustTheOnes # subtract this index from the length of the whole dataframe, this will give the lines to be deleted
    TN_RFG_SCD_cnt_JustTheOnes = TN_RFG_SCD_cnt[:-DeleteLines_JustTheOnes] # delete the lines that have enough data and dont need to be trimmed
    
    DeleteLines = len(TN_RFG_SCD_cnt) - idxDELETE # subtract this index from the length of the whole dataframe, this will give the lines to be deleted
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt[:-DeleteLines] # delete the lines that have enough data and dont need to be trimmed
    
    if DoneTrimming1 == 1 and LoopCount != 0: # After working on trimming the combos with 1 datapoint, we need to purge the dataset of any remaining combos with only 1 datapoint
        TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['DataPoints'], ascending = False) # sort by the number of datapoints
        TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True) # reset the indexes to be aligned with new sorting
        idxDELETE = (TN_RFG_SCD_cnt.DataPoints.values < 2).argmax() # find the index of the first row where there is still only 1 datapoint
        DeleteLines = len(TN_RFG_SCD_cnt) - idxDELETE # subtract this index from the length of the whole dataframe, this will give the lines to be deleted
        TN_RFG_SCD_cnt = TN_RFG_SCD_cnt[:-DeleteLines] # delete the lines that have still only had one data point after first round of trimming
        
        # Now we need to decide what actually needs to be trimmed in this list
        RFG_SCD_cnt = TN_RFG_SCD_cnt.groupby(['RFG', 'lengthRFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'}) # count the number of occurences of each combination in the data
        RFG_SCD_cnt = RFG_SCD_cnt.sort_values(['DataPoints']) # Re-sort
        RFG_SCD_cnt = RFG_SCD_cnt.reset_index(drop=True) # reset the indexes to be aligned with new sorting
        idxDELETE = (RFG_SCD_cnt.DataPoints.values > 4).argmax() # find the index of the first row where there is enough data
        DeleteLines = len(RFG_SCD_cnt) - idxDELETE # subtract this index from the length of the whole dataframe, this will give the lines to be deleted
        RFG_SCD_cnt = RFG_SCD_cnt[:-DeleteLines] # delete the lines that have enough data and dont need to be trimmed
        
        # Now get the indexes so we can find out how many intervals there are actually for the RFG/EvCl combo
        lstNeedsMoreData = [] # initialize a list for saving the indexes of the combos that will still need RFG trimming
        for h in range(len(RFG_SCD_cnt)):
            lstTotalDatapoints = [] # Initialize the list for saving all of the datapoint values
            curRFG = RFG_SCD_cnt.iloc[h,0]
            curEventClass = RFG_SCD_cnt.iloc[h,2]
            curOccurences = RFG_SCD_cnt.iloc[h,3]
            lstCurrentCountIndexes = TN_RFG_SCD_cnt[(TN_RFG_SCD_cnt.RFG == curRFG) & (TN_RFG_SCD_cnt.EventClass == curEventClass)].index.tolist() # look through data and grab indexes of lines that match what we're looking for
            lstTotalDatapoints = TN_RFG_SCD_cnt.iloc[lstCurrentCountIndexes,4]
            if (sum(lstTotalDatapoints) - curOccurences) < 5:
                lstNeedsMoreData.extend(lstCurrentCountIndexes)
        TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.iloc[lstNeedsMoreData]
    
    # Sort by length of RFG
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['lengthRFG'], ascending = False) # sort by the length of the RFG, from largest to smallest -- doing this to always begin trimming from the most specific RFG (specific --> general)
    TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True) # reset the indexes to be aligned with new sorting
    
    if (DoneTrimming1 == 0 and TN_RFG_SCD_cnt.iloc[0,2] < 3) or LoopCount == 0: # We want to check if RFGs are still long enough to be trimmed
        DoneTrimming1 = 1
        if LoopCount != 0: # if this is equal to zero, then we alread saved LoopCount to LoopCount1 and reset LoopCount to 0... we don't want to make LoopCount1 also reset to 0
            LoopCount1 = LoopCount # Save off the loop counter
        LoopCount = 0 # Reset the loop count since we are on the new section of looping, we will add back the loop count each time to keep counting up
        # Need to reset the dataframe so that we stay in the while loop after finishing with the ONES
        TN_RFG_SCD_cnt = NewIdAH64E_2013toPres_SCORED.groupby(['TailNumber', 'RFG', 'lengthRFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'}) # count the number of occurences of each combination in the data
        TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['lengthRFG'], ascending = False) # sort by the length of the RFG, from largest to smallest
        TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True) # reset the indexes to be aligned with new sorting
        # This will only happen once (the first time through where we have finished with the ONES) -- it will not affect next loop because LoopCount was reset and it will pass over trimming section as if it were the first loop again
    
    # Increase the LoopCount by 1 to record another iteration has occured
    LoopCount = LoopCount + 1 # plus-one, and back to the start of the while loop, unless it's time to break out of the loop
    # looping as long as there's data in TN_RFG_SCD_cnt (at this point of loop, we have deleted the lines that have enough datapoints for the analysis), and the first row's RFG is longer than 2 characters (meaning it can be trimmed further)
    # The most likely scenario is that there will still be lines that do not have enough datapoints even after being trimmed to an RFG of only 2 characters, meaning the while loop will break when the first (largest due to sorting) RFG in the dataframe is only 2 characters long


################################################################################################
######################################## End While Loop ########################################
################################################################################################
print("Done Find RFG Trimming ", '--', datetime.now()) # printing this string as the status
# Put the lists that show what has been trimmed into a df
colNames = ['TailNumber', 'RFG', 'EventClass'] # these will be the column names for the new dataframe
Data_for_Weibull = pd.DataFrame(lstTrimmedInfo, columns=colNames) # create the dataframe
Data_for_Weibull = Data_for_Weibull.sort_values(['TailNumber', 'RFG', 'EventClass']) # sort
Data_for_Weibull = Data_for_Weibull.reset_index(drop=True) # reset the index for the new sorting
Data_for_Weibull.to_csv('AH64E_TrimmedData.csv') # save this to a csv

# Save this data, it will be needed again for the Weibull Analysis -- will need to avoid gaps while gathering interval data
GapIssues_TN_RFG_SCD_cnt.to_csv('AH64E_GapIssues_TN_RFG_SCD_cnt.csv') # let's save this data to a csv

## Save the trimmed RFGs that are still too sparse for Weibull
#Trimmed_Still_Sparse1 = NewIdAH64E_2013toPres_SCORED.groupby(['TailNumber', 'RFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'}) # count the number of occurences of each combination in the data
#Trimmed_Still_Sparse1 = Trimmed_Still_Sparse1.sort_values(['DataPoints'])  # sort by the number of datapoints
#Trimmed_Still_Sparse1 = Trimmed_Still_Sparse1.reset_index(drop=True) # reset the indexes to be aligned with new sorting
## Need to use the flags from the final loop to subtract 1 from the datapoints that lose an interval because of the gap
#for e in range(len(GapIssues_TN_RFG_SCD_cnt)): # gonna loop through the ones that need will lose a datapoint because of the gap
#    curTailNumber = GapIssues_TN_RFG_SCD_cnt.iloc[e,0] # save the current tail number to a variable
#    curRFG = GapIssues_TN_RFG_SCD_cnt.iloc[e,1] # save the current RFG to a variable
#    curEventClass = GapIssues_TN_RFG_SCD_cnt.iloc[e,3] # save the current event classification to a variable
#    lstCurrentIndex = Trimmed_Still_Sparse1[(Trimmed_Still_Sparse1.TailNumber == curTailNumber) & (Trimmed_Still_Sparse1.RFG == curRFG) & (Trimmed_Still_Sparse1.EventClass == curEventClass)].index.tolist() # look through data and grab the index of the line that matches what we're looking for
#    Trimmed_Still_Sparse1.iloc[lstCurrentIndex[0],3] = Trimmed_Still_Sparse1.iloc[lstCurrentIndex[0],3] - 1 # subtract 1 from the datapoint to account for gap, will need to update this to subtract more than 1 when there is more than 1 gap
## we want to be left with only what wasn't saved by trimming
#idxDELETE = (Trimmed_Still_Sparse1.DataPoints.values > 5).argmax() # find the index of the first row where there are enough datapoints
#DeleteLines = len(Trimmed_Still_Sparse1) - idxDELETE # subtract this index from the length of the whole dataframe, this will give the lines to be deleted
#Trimmed_Still_Sparse1 = Trimmed_Still_Sparse1[:-DeleteLines] # delete the lines that have enough data and dont need to be trimmed

# Concatenate the data that was deemed to be too little data from the while loop
Trimmed_Still_Sparse1 = [TN_RFG_SCD_cnt, TN_RFG_SCD_cnt_JustTheOnes]
Trimmed_Still_Sparse1 = pd.concat(Trimmed_Still_Sparse1)

Trimmed_Still_Sparse1.to_csv('AH64E_Trimmed_Still_Sparse1.csv') # lets just save this list off so we can see what was left behind

# Loop through data that is still sparse and delete those lines from the data
lstDelSparse = [] # Initialize the list
lstDelSparse1 = [] # Initialize the list
for v in range(len(Trimmed_Still_Sparse1)): # looping through the data that was still to sparse for the analysis
    if v in PrintLess: # Using the list from earlier to print status only every so often
        print((v + 1), '/', len(Trimmed_Still_Sparse1), '--', datetime.now()) # printing this string as the status
    curTailNumber = Trimmed_Still_Sparse1.iloc[v,0] # save the current Tail Number from the "still too sparse" df as we loop through
    curRFG = Trimmed_Still_Sparse1.iloc[v,1] # save the current RFG from the "still too sparse" df as we loop through
    curEventClass = Trimmed_Still_Sparse1.iloc[v,3] # save the current Event Classification from the "still too sparse" df as we loop through
    lstDelSparse1 = NewIdAH64E_2013toPres_SCORED[(NewIdAH64E_2013toPres_SCORED.TailNumber == curTailNumber) & (NewIdAH64E_2013toPres_SCORED.RFG == curRFG) & (NewIdAH64E_2013toPres_SCORED.EventClass == curEventClass)].index.tolist() # look through data and grab indexes of lines that match what we're looking for
    lstDelSparse.extend(lstDelSparse1)
    
NewIdAH64E_2013toPres_SCORED = NewIdAH64E_2013toPres_SCORED.drop(NewIdAH64E_2013toPres_SCORED.index[lstDelSparse]) # delete the lines that were indexed as "still too sparse" from the data
NewIdAH64E_2013toPres_SCORED = NewIdAH64E_2013toPres_SCORED.sort_values(['Key13 / RFG / EventClass']) # just resorting to make sure everything is still good
NewIdAH64E_2013toPres_SCORED = NewIdAH64E_2013toPres_SCORED.reset_index(drop=True) # reset indexes after deleting lines and resorting data, just to avoid any possible issue with indexing

# Save Data with trimmed RFGs to CSV
All_WeibullData = NewIdAH64E_2013toPres_SCORED # save it to a new name
All_WeibullData = All_WeibullData.sort_values(['TailNumber', 'RFG', 'EventClass', 'KEY13']) # sort the values how we want them
All_WeibullData = All_WeibullData.reset_index(drop=True) # reset the index after the sorting
All_WeibullData.to_csv('AH64E_All_WeibullData_TrimRFGs.csv') # go ahead and save it off

# Get final full list of counts for the all the Data by RFG and EventClass combination
WeibullParams = NewIdAH64E_2013toPres_SCORED.groupby(['TailNumber', 'RFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'}) # count the number of occurences of each combination in the data
WeibullParams = WeibullParams.sort_values(['DataPoints']) # sort by the number of datapoints
WeibullParams = WeibullParams.reset_index(drop=True) # reset the index after the sorting
WeibullParams.to_csv('AH64E_WeibullParams_TrimRFGs.csv') # go ahead and save it off

print("Done making the intervals ", '--', datetime.now()) # printing this string as the status
##############################################################################
###################################       ####################################
###############################   Section 2   ################################
###################################       ####################################
######### Go through the trimmed data to gather the interval data ############
###### Is the time to failure for each RFG, is for the wiebull analysis ######
##############################################################################


# Create df with counts for the datapoints
WeibullParams = WeibullParams.sort_values(['TailNumber', 'RFG', 'EventClass']) # re-sort the DF
WeibullParams = WeibullParams.reset_index(drop=True) # re-index for the newly sorted df

lstDiff = [] # Initialize the list
# This speeds up the for loop. All of the data is sorted the same way, so there is no need to search
# the whole dataframe once the loop finds section of the data it is looking for
curTNidxStart = 0 # initialize the variable for saving an index
# Get the Time to Failures from the Weibull capable data
for q in range(len(WeibullParams)):
    if q in PrintLess: # Using the list from earlier to print status only every so often
        print((q + 1), '/', len(WeibullParams), '--', datetime.now()) # print this string as a status
    curTailNumber = WeibullParams.iloc[q,0] # Save the current tail number to a variable
    curRFG = WeibullParams.iloc[q,1] # Save the current RFG to a variable
    curEventClass = WeibullParams.iloc[q,2] # Save the current Event Classification to a variable
    Age = 0 # Initialize the variable for the current line's age --- age is the flight hours for the tail number at the point of the relevant maintenance event
    BegAge = 0 # Initialize the variable for the beginning age
    EndAge = 0 # Initialize the variable for the ending age
    Diff = 0 # Initialize the variable for the difference between the beginning and ending ages
    FoundTN = 0 # Initialize the flag for when we've found the current tail nuber
    FoundRFG = 0 # Initialize the flag for when we've found the current RFG
    FoundEventClass = 0 # Initialize the flag for when we've found the current event classification
    for r in range(curTNidxStart, len(All_WeibullData)): # starting the for loop to get the beginning/ending ages and the difference between them
        if curTailNumber == All_WeibullData.iloc[r,2]: # check to see if the current tail number from Weibull Params is the TN we're looking at in the data
            if FoundTN == 0: # check to see if we've already found the current tail number from WeibullParams
                curTNidxStart = r # if so, then we need to update our starting index to be this point in the data, so we don't have to start from index 0 next time
                FoundTN = 1 # and we need to go ahead and flag that we have found the current tail number from WeibullParams
            if curRFG == All_WeibullData.iloc[r,9]: # check to see if the current RFG from Weibull Params is the RFG we're looking at in the data
                # why didn't I save a new "index start" value here, like whe we found the current tail number ???
                FoundRFG = 1 # if so, we need to go ahead and flag that we have found the current RFG from WeibullParams
                if curEventClass == All_WeibullData.iloc[r,10]: # check to see if the current event classification from WeibullParams is the event classification we're looking at in the data
                    # if so, that means that we have found a line that matches for TN, RFG, and EventClass... yay.
                    if (All_WeibullData.iloc[r,4] == All_WeibullData.iloc[r,5]) or (All_WeibullData.iloc[r,5] != All_WeibullData.iloc[r,5]): # let's check to see if the "RELEVANT_BEG_AGE" is the same as the "SUGG_HOURS_OPEVENTS" OR if "SUGG_HOURS_OPEVENTS" is NaN
                        Age = All_WeibullData.iloc[r,4] # if they are the same, cool, let's just go with the value in the "RELEVANT_BEG_AGE" column
                    else: # but if they are not the same values
                        Age = All_WeibullData.iloc[r,5] # let's go with the value in the "SUGG_HOURS_OPEVENTS" column
                    if FoundEventClass == 0: # if we haven't already flagged that we found the current Event Classification from WeibullParams... then
                        BegAge = Age # let's go ahead and save the 'Age' as the beginning age
                        EndAge = Age # and the ending age -- I think I did this so that we would not record differences on the first pass through, we just grab the age
                        BegDtTm = All_WeibullData.iloc[r,3] # let's go ahead and save the date-time as the beginning date-time -- this will be used later to delete the interval data that's compromised by gaps
                        EndDtTm = BegDtTm # and the ending date-time -- just following the method I implemented previously for BegAge/EndAge -- this will be used later to delete the interval data that's compromised by gaps
                    else: # if we have already flagged that we found the current Event Classification from WeibullParams... then
                        # last time through we we looking at the right combination of TN, RFG, and EventClass
                        BegAge = EndAge # so let's use the age from the previous line as the beginning age
                        EndAge = Age # and let's use the age from this line for the ending age
                        BegDtTm = EndDtTm # so let's use the date-time from the previous line as the beginning date-time -- this will be used later to delete the interval data that's compromised by gaps
                        EndDtTm = All_WeibullData.iloc[r,3] # and let's use the date-time from this line for the ending date-time -- this will be used later to delete the interval data that's compromised by gaps
                    Diff = EndAge - BegAge # now let's take the difference between the two ages
                    
                    GapFlag = 0 # initialize the gap flag as not being raised, so it resets every time before we check for the gap
                    curTNindex = AH64E_ScoredDtTm_GAPS[(AH64E_ScoredDtTm_GAPS.TailNumber == curTailNumber)].index.tolist() # look through AH64E_ScoredDtTm_GAPS and grab the index of the line that matches the TN we're looking for
                    if len(curTNindex) == 1: # check to see that we actually found the current TN in the list of TNs that have gaps
                        # When there's a data gap, there will not be any datapoints within the gap range
                        # The only scenario where the gap is an issue is when the Beginning Date-Time is before the Gap and the Ending Date-Time is after the Gap
                        if AH64E_ScoredDtTm_GAPS.iloc[curTNindex[0],1] >= BegDtTm and AH64E_ScoredDtTm_GAPS.iloc[curTNindex[0],2] <= EndDtTm: # check to see if this interval coincides with a gap for this TN
                            GapFlag = 1 # looks like there's a gap issue, so let's flag it so that we don't record this interval
                    
                    curTNidxStart = r # save the index so we can start from here on the next loop
                    FoundEventClass = 1 # raise the flag that the current Event classification from WeibullParams was found
                elif FoundEventClass == 1: # if the current event classification from WeibullParams was not the event classification we were looking at in the data
                    # and the flag for event classification was raised, that means we've moved out of the section of the data where we've found the right combination of TN, RFG, and EventClass
                    break # so let's break the loop and go to the next combination of values in from WeibullParams
            elif FoundRFG == 1: # if the current RFG from WeibullParams was not the RFG we were looking at in the data
                # and the flag for RFG was raised, that means we've moved out of the section of the data where we've found the right combination of TN, RFG, and EventClass
                break # so let's break the loopand go to the next combination of values in from WeibullParams
        elif FoundTN == 1: # if the current TN from WeibullParams was not the TN we were looking at in the data
                # and the flag for TN was raised, that means we've moved out of the section of the data where we've found the right combination of TN, RFG, and EventClass
            break # so let's break the loop and go to the next combination of values in from WeibullParams
#        if Diff > 0 and GapFlag == 0: # Only record the Time to Fail and the accompanying descriptive info, if the Difference is not 0 and there is not a Gap Issue
        if GapFlag == 0 and Diff >= 0: # Only record the Time to Fail and the accompanying descriptive info, if there is not a Gap Issue and if the difference is greater or equal to 0
            lstDiff.append([curTailNumber, curRFG, curEventClass, Diff]) # appending to a list to have the needed combination of descriptive info as well as the accompanying age difference between maint entries (interval data)

# Save data for weibull to a dataframe to be consolidated by RFG and EventClass
colNames = ['TailNumber', 'RFG', 'EventClass', 'IntervalData'] # these will be the column names for the new dataframe
Data_for_Weibull = pd.DataFrame(lstDiff, columns=colNames) # create the dataframe
Data_for_Weibull = Data_for_Weibull.sort_values(['RFG', 'EventClass']) # Now let's sort by RFG and Event Classification, not TN because TN was only used to get the interval data for the RFG/EventClass combinations
Data_for_Weibull = Data_for_Weibull.reset_index(drop=True) # reset the index for the new sorting
Data_for_Weibull = Data_for_Weibull.drop(columns = ['TailNumber']) # go ahead and drop Tail number, cause we don't need it anymore
Data_for_Weibull.to_csv('AH64E_Data_for_Weibull_TrimRFGsRollTNs.csv') # save this to a csv


# Count the number of zeros for each combination
ZeroCounts = Data_for_Weibull.groupby(['RFG', 'EventClass', 'IntervalData']).size().reset_index().rename(columns={0:'DataPoints'}) # count the number of occurences of each combination in the data
# Gotta delete the non-zeros from this list cause we dont care about them right now
DelNonZeros = ZeroCounts[(ZeroCounts.IntervalData != 0)].index.tolist() # This gets the list of indeces to be deleted (where the values dont equal 0)
ZeroCounts = ZeroCounts.drop(ZeroCounts.index[DelNonZeros]) # Delete those from the list
ZeroCounts = ZeroCounts.sort_values(['RFG', 'EventClass']) # Now let's sort by RFG and Event Classification
ZeroCounts = ZeroCounts.reset_index(drop=True) # reset the index for the new sorting

# Now that we have the ZeroCounts, we can delete them from the actual interval dataset
ZeroIntervals = Data_for_Weibull[(Data_for_Weibull.IntervalData == 0)].index.tolist() # Save the indexes of the zeros to a list
Data_for_Weibull = Data_for_Weibull.drop(Data_for_Weibull.index[ZeroIntervals]) # Delete those from the list
Data_for_Weibull = Data_for_Weibull.sort_values(['RFG', 'EventClass']) # Now let's sort by RFG and Event Classification
Data_for_Weibull = Data_for_Weibull.reset_index(drop=True) # reset the index for the new sorting
print("Done Section 2 ", '--', datetime.now()) # printing this string as the status

##############################################################################
###################################       ####################################
###############################   Section 3   ################################
###################################       ####################################
##############################################################################


# Create a new data point count for the weibull analysis loop
WeibullDataCount = Data_for_Weibull.groupby(['RFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'})
WeibullDataCount = WeibullDataCount.sort_values(['DataPoints'], ascending = False)
WeibullDataCount = WeibullDataCount.reset_index(drop=True)

# Delete the data that does not have enough for weibull analysis
idxDELETE = (WeibullDataCount.DataPoints.values < 5).argmax()
if idxDELETE > 0:
    DeleteLines = len(WeibullDataCount) - idxDELETE
    WeibullDataCount = WeibullDataCount[:-DeleteLines]

# Re-sort and Re-index the df
WeibullDataCount = WeibullDataCount.sort_values(['RFG', 'EventClass'])
WeibullDataCount = WeibullDataCount.reset_index(drop=True)


lstSHAPE = []       # Initialize the list
lstLOCATION = []    # Initialize the list
lstSCALE = []       # Initialize the list
lstMEAN = []        # Initialize the list
lstVAR = []         # Initialize the list
lstSTDEV = []       # Initialize the list
lstDelete = []      # Initialize the list
lstDiff = []        # Initialize the list
lstStat_Pearson = []        # Initialize the list
lstPVal_Pearson = []        # Initialize the list
lstStat_LogLike = []        # Initialize the list
lstPVal_LogLike = []        # Initialize the list
lstStat_FreeTuk = []        # Initialize the list
lstPVal_FreeTuk = []        # Initialize the list
lstStat_ModLogLike = []        # Initialize the list
lstPVal_ModLogLike = []        # Initialize the list
lstStat_Neyman = []        # Initialize the list
lstPVal_Neyman = []        # Initialize the list
lstStat_CRead = []        # Initialize the list
lstPVal_CRead = []        # Initialize the list
#lstZeroCount = []   # Initialize the list
#lstZeroRatio = []   # Initialize the list
for w in range(len(WeibullDataCount)):
    if w in PrintLess: # This is just to print the status message less often
        print((w + 1), '/', len(WeibullDataCount), '--', datetime.now())
    # Get the indexes from the data for the specific RFG-EventClass combo
    lstGrabDiffData = Data_for_Weibull[(Data_for_Weibull.RFG == WeibullDataCount.iloc[w,0]) & (Data_for_Weibull.EventClass == WeibullDataCount.iloc[w,1])].index.tolist()
    lstDiff = Data_for_Weibull.iloc[lstGrabDiffData,2].tolist() # This is just a list of the interval data for the specific RFG-EventClass combo
#    lstDiff = [x+100 for x in lstDiff] # add 100 to all datapoints (this is for playing with the location parameter for a 3-param weibull)
#    lstSubtractLoc = [-100] * len(lstDiff)
    # Do the actual weibull analysis and save the results to the respective lists
    if len(lstDiff) > 1: # Make sure there is more than one non-zero datapoint
        shape, loc, scale = weibull_min.fit(lstDiff)
        Mean, Variance = weibull_min.stats(shape, scale=scale, moments='mv')
        StDev = weibull_min.std(shape, scale=scale)
        Pearson = power_divergence(lstDiff, lambda_='pearson')
        LogLike = power_divergence(lstDiff, lambda_='log-likelihood')
        FreeTuk = power_divergence(lstDiff, lambda_='freeman-tukey')
        ModLogLike = power_divergence(lstDiff, lambda_='mod-log-likelihood')
        Neyman = power_divergence(lstDiff, lambda_='neyman')
        CRead = power_divergence(lstDiff, lambda_='cressie-read')
        lstSHAPE.append(shape)
        lstLOCATION.append(loc)
        lstSCALE.append(scale)
        lstMEAN.append(Mean)
        lstVAR.append(Variance)
        lstSTDEV.append(StDev)
        lstStat_Pearson.append(Pearson[0])
        lstPVal_Pearson.append(Pearson[1])
        lstStat_LogLike.append(LogLike[0])
        lstPVal_LogLike.append(LogLike[1])
        lstStat_FreeTuk.append(FreeTuk[0])
        lstPVal_FreeTuk.append(FreeTuk[1])
        lstStat_ModLogLike.append(ModLogLike[0])
        lstPVal_ModLogLike.append(ModLogLike[1])
        lstStat_Neyman.append(Neyman[0])
        lstPVal_Neyman.append(Neyman[1])
        lstStat_CRead.append(CRead[0])
        lstPVal_CRead.append(CRead[1])
    else: # Place-holder if there is not more than one non-zero datapoint, pre-processing of the data should not allow for this to happen, it is just a final filter
        lstSHAPE.append(-9999.999)
        lstLOCATION.append(-9999.999)
        lstSCALE.append(-9999.999)
        lstMEAN.append(-9999.999)
        lstVAR.append(-9999.999)
        lstSTDEV.append(-9999.999)
        lstStat_Pearson.append(-9999.999)
        lstPVal_Pearson.append(-9999.999)
        lstStat_LogLike.append(-9999.999)
        lstPVal_LogLike.append(-9999.999)
        lstStat_FreeTuk.append(-9999.999)
        lstPVal_FreeTuk.append(-9999.999)
        lstStat_ModLogLike.append(-9999.999)
        lstPVal_ModLogLike.append(-9999.999)
        lstStat_Neyman.append(-9999.999)
        lstPVal_Neyman.append(-9999.999)
        lstStat_CRead.append(-9999.999)
        lstPVal_CRead.append(-9999.999)
        lstDelete.append(w)


# Just need to add in the number of zeros that were deleted during preprocessing
listZeroCounts = []
for v in range(len(WeibullDataCount)):
    curRFG = WeibullDataCount.iloc[v,0] # Save the current RFG to a variable
    curEvC = WeibullDataCount.iloc[v,1] # Save the current EventClass to a variable
    curZeroIdx = ZeroCounts[(ZeroCounts.RFG == curRFG) & (ZeroCounts.EventClass == curEvC)].index.tolist() # grab the index for this RFG/EvClass
    curZeroCount = ZeroCounts.iloc[curZeroIdx[0],3] # use that index to grab the number of zeros that were deleted for that particular RFG/EvClass
    if len(curZeroIdx) == 1:
        listZeroCounts.append(curZeroCount)
    elif len(curZeroIdx) == 0:
        listZeroCounts.append(curZeroCount)


# Put the Shape, Scale, Mean, Variance, and StDev in the dataframe and write it to a CSV
WeibullDataCount['Shape'] = lstSHAPE
WeibullDataCount['Location'] = lstLOCATION
WeibullDataCount['Scale'] = lstSCALE
WeibullDataCount['Mean'] = lstMEAN
WeibullDataCount['Variance'] = lstVAR
WeibullDataCount['St. Dev.'] = lstSTDEV
WeibullDataCount['Zero Count'] = listZeroCounts
WeibullDataCount['Pearson_CRstat'] = lstStat_Pearson
WeibullDataCount['Pearson_Pvalue'] = lstPVal_Pearson
WeibullDataCount['LogLikelihood_CRstat'] = lstStat_LogLike
WeibullDataCount['LogLikelihood_Pvalue'] = lstPVal_LogLike
WeibullDataCount['FreemanTukey_CRstat'] = lstStat_FreeTuk
WeibullDataCount['FreemanTukey_Pvalue'] = lstPVal_FreeTuk
WeibullDataCount['ModLogLikelihood_CRstat'] = lstStat_ModLogLike
WeibullDataCount['ModLogLikelihood_Pvalue'] = lstPVal_ModLogLike
WeibullDataCount['Neyman_CRstat'] = lstStat_Neyman
WeibullDataCount['Neyman_Pvalue'] = lstPVal_Neyman
WeibullDataCount['CressieRead_CRstat'] = lstStat_CRead
WeibullDataCount['CressieRead_Pvalue'] = lstPVal_CRead
WeibullDataCount = WeibullDataCount.drop(WeibullParams.index[lstDelete]) # This (hopefully) will do nothing, due to data preprocessing
WeibullDataCount = WeibullDataCount.sort_values(['RFG', 'EventClass'])
WeibullDataCount = WeibullDataCount.reset_index(drop=True)
WeibullDataCount.to_csv('AH64E_WeibullAnalysis_FreeLoc.csv')




print("Last Part ", '--', datetime.now()) # printing this string as the status








#
#import matplotlib.pyplot as plt
#fig, ax = plt.subplots(1, 1)
#
#
#r = lstDiff
#ax.hist(r, density=True, histtype='stepfilled', alpha=0.2)
#ax.legend(loc='best', frameon=False)
#plt.show()
#
#
#_ = plt.hist(lstDiff, bins=15)  # arguments are passed to np.histogram
#plt.plot(lstDiff, weibull_min.pdf(lstDiff, shape))
#plt.title("00 - SchedMaint")
#
#plt.show()
#






