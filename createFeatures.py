
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


#Read data into pandas
metaDataDf = pd.read_csv('shortmeta.csv')


# In[33]:


raw = pd.read_csv('shortlc.csv')


# In[34]:


#create time series for each index
tsdict = OrderedDict()
nobjects = len(metaDataDf)

pbmap = OrderedDict([(0,'u'), (1,'g'), (2,'r'), (3,'i'), (4, 'z'), (5, 'Y')])
pbnames = list(pbmap.values())

x=0
#Build a timeseries object for each row in the astropy light curve table
print('Creating Time Series Objects...')
for i in range(nobjects):
    row = metaDataDf.iloc[[i]]
    thisid = row.iloc[0,0]
    meta = {'z':row.iloc[0,7],            'zerr':row.iloc[0,8],            'mwebv':row.iloc[0,10]}
    ind = np.asarray([x ==thisid for x in raw['object_id']])
    thislc = raw[ind]
    pbind = [np.asarray(thislc['passband'] == pb) for pb in pbmap]
    t = [np.asarray(thislc.loc[:,'mjd'][mask]) for mask in pbind]  

    m = [np.asarray(thislc['flux'][mask]) for mask in pbind]
    e = [np.asarray(thislc['flux_err'][mask]) for mask in pbind]
    
    tsdict[thisid] = TimeSeries(t=t, m=m, e=e, name=thisid, meta_features=meta,channel_names=pbnames)
    if i % 1000000 == 0:
        print(str(i) + ' done out of ' + str(nobjects))

    
#Empty list for features
features_list = []
print("Generating features from objects and storing to a table...")
with multiprocessing.Pool() as pool:  
    results = pool.imap(worker, list(tsdict.values()))
    i = 0
    for res in results:
        features_list.append(res)
        if i % 1000000 == 0:
            print(str(i) + ' done out of ' + str(nobjects))
        i = i +1


# In[35]:


featurefile = 'C:/Users/Greg/Documents/Personal/PLAsTiCC/test.npz'

featuretable = featurize.assemble_featureset(features_list=features_list,time_series=tsdict.values())
featurize.save_featureset(fset=featuretable, path=featurefile)
print("Feature Table Created.")

