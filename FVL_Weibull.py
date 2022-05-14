import pyodbc
import pandas as pd
from datetime import datetime
from scipy.stats import weibull_min
from scipy.stats import power_divergence

# Threshold DateTime and Hour diff for gap determination
minGapSize_Days = 30
minGapSize_Hours = 10

#region GetData and save copied
AH64E_SCORED_2013toPres = pd.read_csv('AH64E_SCORED_2013toPres.csv')  #Read all the data
AH64E_2013toPres_SCORED = AH64E_SCORED_2013toPres[
                            (AH64E_SCORED_2013toPres['SCD2'] != 'X') & 
                            (AH64E_SCORED_2013toPres['SCD3'] != 'N') & 
                            (AH64E_SCORED_2013toPres['SCD5'] != 'N')
                        ]
AH64E_2013toPres_SCORED.to_csv("output/AH64E_2013toPres_SCORED.csv")

AH64E_TailNumList = AH64E_2013toPres_SCORED['EI_SN'].unique()   
AH64E_RFGList     = AH64E_2013toPres_SCORED['RFG'].unique()
AH64E_SCD1List    = AH64E_2013toPres_SCORED['SCD1'].unique()

tmp1 = pd.DataFrame(AH64E_TailNumList,columns=['EI_SN'])
tmp2 = pd.DataFrame(AH64E_RFGList,columns=['RFG'])
tmp3 = pd.DataFrame(AH64E_SCD1List,columns=['SCD1'])

tmp1.to_csv('output/AH64E_TailNumList.csv')
tmp2.to_csv('output/AH64E_RFGList.csv')
tmp3.to_csv('output/AH64E_SCD1List.csv')
#endregion
##############################################################################
###################################       ####################################
############################   Section: "Gaps"   #############################
###################################       ####################################
############# Gather the data that shows the gaps in scored data #############
##############################################################################

#region Define Gaps
GapFrame = AH64E_SCORED_2013toPres.copy() #work with a copy, not the orig

GapFrame = GapFrame.drop(columns = ['MAL_EFF', 'CORR_DATE_TIME', 'EI_CORR_AGE', 
                                                                'TMMH', 'TMEN', 'TIMH', 'in_phase', 'in_qc', 
                                                                'RFG', 'SCD1', 'SCD2', 'SCD3', 'SCD4', 'SCD5', 
                                                                'SCD6', 'SCD7', 'SCD8', 'SCD9', 'PRIMARY_EVENT'])
try:
   GapFrame = GapFrame.drop(columns = ['RELEVANT_BEG_AGE.1','Unnamed: 0'])  # store as pkl and remove extra RELEVANT_BEG_AGE from sql
except:
    print("Superfluous columns removed already")

# Convert to Datetime to simplify the calculation.   
GapFrame.EVENT_DATE_TIME = pd.to_datetime(GapFrame.EVENT_DATE_TIME) #Seems to work without error on this dataset.  Will see if it continues

# Add a date difference column and give the difference between this date and the last
GapFrame['datediff']  = GapFrame.groupby('EI_SN')['EVENT_DATE_TIME'].diff() 
# Same for the Flight Hours
GapFrame['hoursdiff'] = GapFrame.groupby('EI_SN')['RELEVANT_BEG_AGE'].diff()

# Add previous hours and date to this row so that we can visually compare.  Not strictly necessary
GapFrame['previous_date'] = GapFrame.groupby('EI_SN')['EVENT_DATE_TIME'].shift(1)
GapFrame['previous_hours'] = GapFrame.groupby('EI_SN')['RELEVANT_BEG_AGE'].shift(1)

# Filter the GapFrame by min Gap Size *and* min Gap hours.   Save the result to a pkl (saves more of the structure of the Dataframe with column types) 
# 
TN_GAP_FRAME = GapFrame[
               ((GapFrame.datediff >= pd.Timedelta(str(minGapSize_Days) +  ' days')) & 
                (abs(GapFrame.hoursdiff) >= minGapSize_Hours)) ]
TN_GAP_FRAME.head()
TN_GAP_FRAME.to_pickle('output/TailNumbers_and_GapTimes.pkl')

# TN_GAP_FRAME represents each tail number and the dates between which there is no data (according to the minimums)
# The XXXX_ScoredDt_GAPS frames represent the periods of time for which there *is* data.   To convert I need to invert the time deltas
# This may not be necesssary and is a slight time sink.

AH64E_ScoredDtTm_GAPS = pd.DataFrame()
groups = TN_GAP_FRAME.groupby('EI_SN').groups.keys() #Tail Numbers are the groups
for group in groups :
    # Filter the original dataset to just the tailnumbers we care about and then add a row.
    # Gaps = 
    # [previous_date,EVENT_DATE_TIME], [previous_date,EVENT_DATE_TIME],[previous_date,EVENT_DATE_TIME].....
    # So Non-Gaps = 
    # [Min(EVENT_DATE_TIME),previous_date],[EVENT_DATE_TIME,previous_date],[EVENT_DATE_TIME,previous_date],[EVENT_DATE_TIME,max(EVENT_DATE_TIME)].....
    #
    tmpdf = TN_GAP_FRAME[TN_GAP_FRAME['EI_SN']==group]  
    extrarowdf = tmpdf.iloc[-1:]
    tmpdf = pd.concat([tmpdf,extrarowdf], ignore_index=True)
    tmpdf['StartDtTm'] = tmpdf['EVENT_DATE_TIME'].shift(1) 
    tmpdf['EndDtTm']   = tmpdf['previous_date']
    tmpdf['TailNumber'] = tmpdf['EI_SN']
    
    tmpdf.loc[0,'StartDtTm'] = GapFrame[GapFrame['EI_SN'] == group]['EVENT_DATE_TIME'].min() #             Min(EVENT_DATE_TIME)
    tmpdf.loc[tmpdf.index[-1],'EndDtTm'] = GapFrame[GapFrame['EI_SN'] == group]['EVENT_DATE_TIME'].max() # Max(EVENT_DATE_TIME)

    AH64E_ScoredDtTm_GAPS = pd.concat([AH64E_ScoredDtTm_GAPS,tmpdf[['TailNumber','StartDtTm','EndDtTm']]],ignore_index=True)  #add each TN Non-Gaps to dataframe.


AH64E_ScoredDtTm_GAPS.reset_index(inplace=True)
AH64E_ScoredDtTm_GAPS.drop('index',axis=1,inplace=True)
AH64E_ScoredDtTm_GAPS.to_csv('output/AH64E_ScoredDtTm_GAPS.csv')  # verified it contains the same data as the original.
#endregion

##############################################################################
###################################       ####################################
###############################   Section 0   ################################
###################################       ####################################
################## Just doing some basic Data PreProcessing ##################
##############################################################################

#region PreProcessing
WeibullFrame = AH64E_2013toPres_SCORED.copy()
try:
   WeibullFrame = WeibullFrame.drop(columns = ['RELEVANT_BEG_AGE.1','Unnamed: 0'])  # store as pkl and remove extra RELEVANT_BEG_AGE from sql
except:
    print("Superfluous columns removed already")
# Set all EventClasses according to SCD Rules
# Filter and save dataframe for each EventClass
WeibullFrame.loc[((WeibullFrame.SCD2 != 'N') & (WeibullFrame.SCD2 != 'P') & (WeibullFrame.SCD2 != 'X') & (WeibullFrame.SCD2 != 'Z') & (WeibullFrame.SCD2 != '')) & (WeibullFrame.SCD3 == 'C') & (WeibullFrame.SCD8 != 'N') & (WeibullFrame.RFG != '36B'),'EventClass'] = 'EMA'
ema = WeibullFrame[WeibullFrame['EventClass'] == 'EMA']

WeibullFrame.loc[(WeibullFrame.SCD3 == 'C') & (WeibullFrame.SCD4 != 'O') & (WeibullFrame.SCD4 != 'H') & ((WeibullFrame.SCD2 == 'J') | (WeibullFrame.SCD2 == 'K') | (WeibullFrame.SCD2 == 'C') | (WeibullFrame.SCD2 == 'S') | (WeibullFrame.SCD2 == 'W') | (WeibullFrame.SCD2 == 'Q') | (WeibullFrame.SCD2 == 'U')) & ((WeibullFrame.SCD5 == '1') | (WeibullFrame.SCD5 == '2') | (WeibullFrame.SCD5 == '4')) & (WeibullFrame.RFG != '36B'),'EventClass'] = 'MA'
ma = WeibullFrame[WeibullFrame['EventClass'] == 'MA']

WeibullFrame.loc[((WeibullFrame.SCD2 != 'D') & (WeibullFrame.SCD2 != 'N') & (WeibullFrame.SCD2 != 'P') & (WeibullFrame.SCD2 != 'X') & (WeibullFrame.SCD2 != 'Z') & (WeibullFrame.SCD2 != '')) & (WeibullFrame.SCD3 == 'C') & (WeibullFrame.SCD8 != 'N') & (WeibullFrame.SCD9 != 'N') & (WeibullFrame.RFG != '36B'),'EventClass'] = 'MAF'
maf = WeibullFrame[WeibullFrame['EventClass'] == 'MAF']

WeibullFrame.loc[(WeibullFrame.SCD2 != 'X') & (WeibullFrame.SCD3 == 'C') & (WeibullFrame.SCD5 == 'S') & (WeibullFrame.RFG != '36B'),'EventClass'] = 'SchedMaint'
schedmaint = WeibullFrame[WeibullFrame['EventClass'] == 'SchedMaint']

WeibullFrame.loc[((WeibullFrame.SCD2 != 'X') & (WeibullFrame.SCD2 != 'Z')) & (WeibullFrame.SCD3 == 'C') & ((WeibullFrame.SCD5 != 'M') & (WeibullFrame.SCD5 != 'R') & (WeibullFrame.SCD5 != 'S')) & (WeibullFrame.RFG != '36B'),'EventClass'] = 'UMA'
uma = WeibullFrame[WeibullFrame['EventClass'] == 'UMA']

WeibullFrame.loc[(WeibullFrame.SCD5 != 'S') & (WeibullFrame.SCD2 != 'X') & (WeibullFrame.SCD3 == 'C'),'EventClass'] = 'UnschedMaint'
unschedmaint = WeibullFrame[WeibullFrame['EventClass'] == 'UnschedMaint']

# Concatinate all the EventClasses into a big frame and remove the old frame
WeibullFrame = pd.concat([ema,ma,maf,schedmaint,uma,unschedmaint],ignore_index=True)


WeibullFrame['Key13 / RFG / EventClass'] = WeibullFrame.apply(lambda y: str(y.KEY13[:15] + y.RFG + '-' + y.EventClass + y.KEY13[14:]),axis=1)
WeibullFrame = WeibullFrame.drop(['MAL_EFF', 'CORR_DATE_TIME', 'EI_CORR_AGE', 'in_phase', 'in_qc', 'SCD1','SCD2', 'SCD3', 'SCD4', 'SCD5', 'SCD6', 'SCD7', 'SCD8', 'SCD9', 'PRIMARY_EVENT'], axis=1)
WeibullFrame = WeibullFrame.sort_values(['Key13 / RFG / EventClass'])
WeibullFrame = WeibullFrame.reset_index(drop=True)

WeibullFrame['lengthRFG'] = WeibullFrame.apply(lambda y :len(y['RFG']), axis=1)
WeibullFrame = WeibullFrame.rename(columns={'EI_SN':'TailNumber'})

WeibullFrame = WeibullFrame[['Key13 / RFG / EventClass','KEY13','TailNumber','EVENT_DATE_TIME','RELEVANT_BEG_AGE','TMMH','TMEN','TIMH','RFG','EventClass','lengthRFG']]
WeibullFrame.to_csv('output/AH64E_NewId_2013toPres_SCORED.csv')

# Create dataframe with all RFG and SCD1 combinations and include counts for number of datapoints and Save to CSV
TN_RFG_SCD_cnt = WeibullFrame.groupby(['TailNumber', 'RFG', 'lengthRFG', 'EventClass']).size().reset_index().rename(columns={0:'DataPoints'})
TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.sort_values(['lengthRFG'], ascending = False)
TN_RFG_SCD_cnt = TN_RFG_SCD_cnt.reset_index(drop=True)
TN_RFG_SCD_cnt.to_csv('output/AH64E_TN_RFG_SCD_cnt.csv')

#endregion

##############################################################################
###################################       ####################################
###############################   Section 1   ################################
###################################       ####################################
######### #
##############################################################################Trim RFGs that do not have enough data for the analysis ############
#### Delete any leftovers that still dont have enough data after trimming ###