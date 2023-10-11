#!/usr/bin/env python
# coding: utf-8

# # Data Import: City of Chicago Data Portal

# ## Traffic Data: each record is crash event
#    ### https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if
# ## Traffic Data: Each record describes person involved in event
#    ### https://data.cityofchicago.org/Transportation/Traffic-Crashes-People/u6pd-qa9d
# ## Traffic Data: Each Record describs vehicle involved in event
#    ### https://data.cityofchicago.org/Transportation/Traffic-Crashes-Vehicles/68nd-jvt3

#  

# ### Import Libraries
#    #### Socrata is libary needed to access Chicago API

# In[1]:


import pandas as pd
import requests
import json
import time
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
from sodapy import Socrata
from cdp_secrets import token
print('completed at ' + str(datetime.now()))


# ## Chicago Data Portal Connection API Build

# In[2]:


#Constants
chicago_url="data.cityofchicago.org"
vehicle_api_root="68nd-jvt3"
people_api_root="u6pd-qa9d"
crash_api_root="85ca-t3if"
district_root="z8bn-74gv"
beat_root="n9it-hstw"

#Chicago Data Portal Connection
cdp = Socrata(chicago_url,
                 token
             )
print('completed at ' + str(datetime.now()))


# In[3]:


def callAPI(root: str, filter: str) -> pd.DataFrame:
    results=cdp.get(root, query=filter)
    df=pd.DataFrame.from_records(results)
    return df
print('completed at ' + str(datetime.now()))


# In[4]:


# Get dates for the last 18 months
today = datetime.now().strftime("%Y-%m-%d")
eighteen_months = (datetime.now() - relativedelta(months=18)).strftime("%Y-%m-%d")

print(today)
print(eighteen_months)
print('completed at ' + str(datetime.now()))


# In[5]:


#limit required for SoQL query, as we are filtering for one year limit is set high to capture all records
limit='1000000000'

#Filters use SQL structure, some where clauses use a different format. Refer to API Documentation for correct syntax
vehicle_filter= """Select crash_record_id,crash_date,make,model,vehicle_type 
                    where vehicle_type is not null and crash_date between '%s' and '%s'
                    limit %s"""%(eighteen_months,today,limit) 

people_filter= """Select person_id,crash_record_id,crash_date,person_type,age,sex,
                injury_classification,pedpedal_action,pedpedal_visibility,pedpedal_location
                Where CRASH_DATE between '%s' and '%s' and
                (person_type='PEDESTRIAN' or person_type='BICYCLE') limit %s"""% (eighteen_months,today,limit)

crash_filter="""Select *
                where crash_date between '%s' and '%s' limit %s"""% (eighteen_months,today,limit)

district_filter="""Select distinct district,district_name limit 1000"""

beat_filter="""Select beat_num,district limit 300"""

print('completed at ' + str(datetime.now()))


# In[6]:


#People Table
people_df=callAPI(people_api_root,people_filter)

print('completed at ' + str(datetime.now()))


# In[7]:


#Vehicle Table
vehicle_df=callAPI(vehicle_api_root,vehicle_filter)

print('completed at ' + str(datetime.now()))


# In[8]:


#Crash Table
crash_df=callAPI(crash_api_root,crash_filter)

print('completed at ' + str(datetime.now()))


# In[ ]:


#Beat To District Key Table
#To be Replaced with neighborhood table

#Police Beat
beat_df=callAPI(beat_root,beat_filter)

#Remove leading 0 from district field
beat_df['district']=pd.to_numeric(beat_df['district'])
beat_df['district']=beat_df['district'].apply(lambda x: str(x))

#Remove leading 0 from beat_num field
beat_df['beat_num']=pd.to_numeric(beat_df['beat_num'])
beat_df['beat_num']=beat_df['beat_num'].apply(lambda x: str(x))

#District table
district_df=callAPI(district_root,district_filter)

#join together to create matching table
district_key=pd.merge(beat_df,district_df,on='district',how='left')

print(district_key)
print('completed at ' + str(datetime.now()))


# In[ ]:



#Join People and Vehicle data sets on crash_record_id
people_vehicle=pd.merge(people_df,vehicle_df[['crash_record_id','make','model','vehicle_type']], on='crash_record_id', how='left')
#print(people_vehicle)

#Join prior dataset with crash information
crash_pv=pd.merge(people_vehicle,crash_df, on='crash_record_id', how='left')
#print(crash_pv)

#Join prior dataset with district ID and Name
#merge_df=pd.merge(crash_pv,district_key, on='beat_num', how='left')
merge_df = pd.merge(crash_pv, district_key, left_on='beat_of_occurrence', right_on='beat_num', how='left')
#Declare final data model
model=merge_df


#print(model)
print("Model Field List \n")
n=1
for fields in model.columns:
    print(str(n)+'.'+fields)
    n+=1

print('completed at ' + str(datetime.now()))


# In[ ]:


model
model.to_excel('pedestrian_incidents.xlsx',index=True)
print('completed at '+str(datetime.now()))

