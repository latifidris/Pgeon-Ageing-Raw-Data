# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 15:54:33 2022

@author: LATIF
"""

import os

os.chdir("C:/Users/LATIF/OneDrive/Desktop/Python")

import pandas as pd
import numpy as np

file = pd.read_csv('File/Ageing_20220809.csv')
ref1 = pd.read_csv('Ref/References1.csv')
ref2 = pd.read_csv('Ref/References2.csv')
ref3 = pd.read_csv('Ref/References2.csv')

file.rename(columns = {'tracking_code' : 'AWB'}, inplace = True)
ref1.rename(columns = {'Location':'last_checkin_hub', 'Code':'L.Hub'},inplace=True)
ref2.rename(columns = {'Postcode':'sender_postcode','Region':'O.Region', 'O.Code':'O.Hub'},inplace=True)
ref3.rename(columns = {'Postcode':'receiver_postcode','Region':'D.Region', 'D.Code':'D.Hub'},inplace=True)

join1 = pd.merge(file, ref1, on = 'last_checkin_hub', how='left')
join2 = pd.merge(join1, ref2, on = 'sender_postcode', how='left')
df = pd.merge(join2, ref3, on = 'receiver_postcode', how='left')

df[['L.Hub','O.Region','O.Hub','D.Region','D.Hub']] = df[['L.Hub','O.Region','O.Hub','D.Region','D.Hub']].fillna('-')
df = df.fillna('')

df['is_return'] = np.where(df['return-initial'] == '', 'No', 'Yes')  
df['Scan'] = np.where(df['origin-checkin'] == '', 'No', 'Yes')
df['OFD'] = np.where(df['first-ofd'] == '', 'No', 'Yes')
df['Attempt'] = np.where(df['first-delivery-attempt'] == '', 'No', 'Yes')
df['Complete'] = np.where(df['complete-at'] == '', 'No', 'Yes')
df['Current date'] = pd.to_datetime('today')  

df['return-initial'] = pd.to_datetime(df['return-initial']).dt.date
df['pickup-at'] = pd.to_datetime(df['pickup-at']).dt.date
df['origin-checkin'] = pd.to_datetime(df['origin-checkin']).dt.date
df['dest-hub-first-checkin'] = pd.to_datetime(df['dest-hub-first-checkin']).dt.date
df['first-ofd'] = pd.to_datetime(df['first-ofd']).dt.date
df['first-delivery-attempt'] = pd.to_datetime(df['first-delivery-attempt']).dt.date
df['complete-at'] = pd.to_datetime(df['complete-at']).dt.date
df['Current date'] = pd.to_datetime(df['Current date']).dt.date

age_1 = (df['Current date'] - df['dest-hub-first-checkin']).dt.days
age_2 = (df['Current date'] - df['pickup-at']).dt.days
d_1 = (df['first-delivery-attempt'] - df['dest-hub-first-checkin']).dt.days
d_2 = (df['complete-at'] - df['dest-hub-first-checkin']).dt.days

df['Age'] = np.where(df['Scan'] == 'Yes', age_1, age_2)
df['Age Group'] = np.where(df['Age'] > 13, '14 & Above', df['Age'])
df['D'] = np.where(df['Attempt'] == 'Yes', d_1, d_2)

df = df.fillna('null')
df = df.replace("", "null")

def dfunc(df):
    if df['D'] == 3:
        return 'D+3'
    elif df['D']==2:
        return 'D+2'
    elif df['D']==1:
        return 'D+1'
    elif df['D']!='null' and df['D']<=0:
        return 'D+0'
    elif df['D']!='null' and df['D']>=3:
        return '>D+3'
    elif df['D']=='null' and df['status']=='completed':
        return 'D+2'
    elif df['D']=='null' and df['status']!='completed':
        return 'Ageing'
    else:
        pass

df['D+'] = df.apply(dfunc, axis=1)

df = df.drop_duplicates(subset=['AWB'])

qaqc_formula = '=IF(ISNUMBER(SEARCH("Delivered",V2))=TRUE,"Delivered",IF(ISNUMBER(SEARCH("Returned",V2))=TRUE,"Returned",IF(ISNUMBER(SEARCH("Dropped off ",V2))=TRUE,"Dropped off at Pegon Point", IF(AND(B2="completed",J2="null"),"Change AWB/Lost/Dispose/Damage/Self-collect", IF(AND(ISNUMBER(SEARCH("Checked in", V2))=TRUE, M2=Q2,M2<>U2,C2="No"), IF(AH2="null",CONCAT("Not yet received from ",Q2),AH2),IF(AND(ISNUMBER(SEARCH("Checked in",V2))=TRUE, M2<>Q2,M2<>U2,C2="No"), CONCAT("Misroute to Hub ",M2),IF(AND(ISNUMBER(SEARCH("Parcel has been collected",V2))=TRUE,X2="No"),CONCAT("Parcel collected by driver. Yet to check in by Origin Hub ",Q2),IF(AND(OR(ISNUMBER(SEARCH("Collection attempted.",V2))=TRUE,ISNUMBER(SEARCH("Parcel has been collected",V2))=TRUE),X2="Yes",C2="No"),"Driver misupdated. Does not follow SOP",IF(C2="Yes","Return is initiated.",IF(AND(ISNUMBER(SEARCH("Checked in",V2))=TRUE,M2=U2,C2="No"),IF(AH2="null",CONCAT("Pending at Destination Hub ",U2),IF(ISNUMBER(SEARCH("Delivery attempted. ",AH2))=TRUE,TRIM(RIGHT(AH2,LEN(AH2)-SEARCH(". ",AH2))),AH2)), IF(ISNUMBER(SEARCH("Delivery attempted. ",V2))=TRUE,TRIM(RIGHT(V2,LEN(V2)-SEARCH(". ",V2))),V2)))))))))))'
df['QAQC Remarks'] = ''
df['QAQC Remarks'] = df['QAQC Remarks'].iloc[:1].replace('', qaqc_formula)

df_final = df[['AWB','status','is_return',
              'return-initial','pickup-at',
              'origin-checkin','dest-hub-first-checkin','first-ofd',
              'first-delivery-attempt','complete-at',
              'first_checkin_hub','last_checkin_hub',
              'L.Hub', 'sender_postcode','sender_state_name',
              'O.Region','O.Hub','receiver_postcode',
              'receiver_state_name','D.Region','D.Hub',
              'latest_tracking','QAQC Remarks','Scan',
              'OFD','Attempt','D','D+','Age','Age Group',
              'collection_attempt_count','collection_attempt_reason',
              'delivery_attempt_count','delivery_attempt_reason']]


df_final.to_csv('raw_data_Ageing_20220809.csv', index=False)

