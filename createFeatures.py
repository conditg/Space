
# coding: utf-8

# In[31]:


from collections import Counter, OrderedDict
import numpy as np
from operator import itemgetter
import multiprocessing
from cesium.time_series import TimeSeries
import cesium.featurize as featurize
import pandas as pd
from testWorker import worker


# In[32]:


#Read data into pandas. This metadata file has one row for each object and 8-10 columns of 
#data pertaining to object position and galaxy brightness.
# If Training set = ~ 8K rows
# If Test set = ~ 3.5M rows
metaDataDf = pd.read_csv('shortmeta.csv')


# In[33]:

#Read the time series data into pandas. This file has and variable number of rows for each object in 
#the metadata file. Each row is an observation made by the telescope, with a unique timestamp, an indicator
#for the passband use (6 possibilities), and the flux measured and it's error. 
# If Training set = ~ 1.5M rows
# If Test set = ~ 450M rows

raw = pd.read_csv('shortlc.csv')


# In[34]:


#create time series for each index. This will ultimately store each object_id, and it's corresponding TimeSeries() object
tsdict = OrderedDict()

#store the # of objects we are working with
nobjects = len(metaDataDf)

#Map the passband integers to the astronomic labels - these correspond to ranges on the spectrum of light waves
#these will be used to organize and label the data in the TimeSeries() object
pbmap = OrderedDict([(0,'u'), (1,'g'), (2,'r'), (3,'i'), (4, 'z'), (5, 'Y')])
pbnames = list(pbmap.values())

x=0
#Build a timeseries object for each object_id based on the time series data

print('Creating Time Series Objects...')
for i in range(nobjects):
    #TimeSeries() Objects need to receive as input specific data about each object, which we extract below:
    # extract the row of metadata for object_id i
    row = metaDataDf.iloc[[i]]  
    
    #store it's object_id, which will be used to label it
    thisid = row.iloc[0,0] 
    
    #Next, hold the meta data out as a dictionary. TimeSeries() needs to receive this separate from time series measurements.
    meta = {'z':row.iloc[0,7],'zerr':row.iloc[0,8],'mwebv':row.iloc[0,10]}
    
    #Create a boolean mask based on the object_id to apply to the time series data
    ind = np.asarray([x ==thisid for x in raw['object_id']])
    thislc = raw[ind]
    
    #create a boolean mask that will reorganize the time series data by passband
    #this mask will be a list with length 6 (one for each passband). Each of the 6 items is another list
    #where the length equals how many total observations that object has in the time series data
    pbind = [np.asarray(thislc['passband'] == pb) for pb in pbmap]
    
    #Store the times of all the measurements for this object_id. this will be a list with length 6 (one for
    # each passband) where each of the 6 items is a list of length = how many observations that object
    #has for JUST THAT PASSBAND in the time series data
    t = [np.asarray(thislc.loc[:,'mjd'][mask]) for mask in pbind]  
    
    #Store the flux measurements taken for this object_id. This is the same shape as t
    m = [np.asarray(thislc['flux'][mask]) for mask in pbind]
    
    #Store the flux measurement errors for this object_id. This is the same shape as t
    e = [np.asarray(thislc['flux_err'][mask]) for mask in pbind]
    
    #Create the TimeSeries() object using the variables defined above, and append it to the ordered dict.
    tsdict[thisid] = TimeSeries(t=t, m=m, e=e, name=thisid, meta_features=meta,channel_names=pbnames)
    
    #Occasionally print progress so we can assess speed
    if i % 1000000 == 0:
        print(str(i) + ' done out of ' + str(nobjects))

    
#Empty list for the actual features we want the TimeSeries() object to facilitate
features_list = []
print("Generating features from objects and storing to a table...")

with multiprocessing.Pool() as pool:  
    #Apply the worker function to each object in the dict of TimeSeries() objects. see ./testWorker.py
    #this returns a single row for each object with hundreds of features determined by ./testWorker.py
    results = pool.imap(worker, list(tsdict.values()))
    i = 0
    for res in results:
        #append the created row of features to our list
        features_list.append(res)
        if i % 1000000 == 0:
            print(str(i) + ' done out of ' + str(nobjects))
        i = i +1


# In[35]:

#With features generated for all objects, write the feature list to a file so that we don't need to run this again
featurefile = 'C:/Users/Greg/Documents/Personal/PLAsTiCC/test.npz'
featuretable = featurize.assemble_featureset(features_list=features_list,time_series=tsdict.values())
featurize.save_featureset(fset=featuretable, path=featurefile)
print("Feature Table Created.")

