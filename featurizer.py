import cesium.featurize as featurize

#store a list of all the features we want to use in our models. 
#options here: http://cesium-ml.org/docs/feature_table.html


features_to_use = ["all_times_nhist_numpeaks",
                   "amplitude",
                   "percent_beyond_1_std",
                   "maximum",
                   "max_slope",
                   "median",
                   "median_absolute_deviation",
                   "percent_close_to_median",
                   "period_fast",
                   "qso_log_chi2_qsonu",
                   "freq1_signif",
                   "freq1_freq",
                   "linear_trend",
                   "minimum",
                   "skew",
                   "std",
                   "weighted_average"]

#With a list of the starting positions by object (len = 3.5M)
positions = [1,12076,24634,...]

#store pb names for labelling the features accurately
pbnames=['u', 'g', 'r', 'i', 'z', 'Y']

#loop through each object
for i in len(positions):
    #seek to it's starting position
    f.seek(positions[i])
    #read up to the start of the subsequent object, split by row
    s = f.read(positions[i+1] - positions[i]).split('/n')
    #assign id, create empty lists for t/m/e
    idnum= s.split(',')[0]
    t=[[],[],[],[],[],[]]
    m=[[],[],[],[],[],[]]
    e=[[],[],[],[],[],[]]
    #Append each t/m/e value to the right passband list  within t/m/e/ lists
    for row in s.split(','):
        pb = row[2]
        t[pb].append(row[1])
        m[pb].append(row[3])
        e[pb].append(row[4])
        #create ts obj, then generate features
    tsobj = TimeSeries(t=t, m=m, e=e, name=idnum, channel_names=pbnames)
    thisfeats = featurize.featurize_single_ts(
            tsobj, features_to_use=features_to_use, raise_exceptions=False
        ) #this is a dict where keys are feature names and values are the respective scalars
    with open('featfile.csv','a') as ff:
        ff.write(thisfeats) #Need to check if this would actually work. 
    del s, idnum, pb,t, m, e, tsobj, thisfeats
        
        
        



