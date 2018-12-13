import cesium.featurize as featurize
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

def worker(tsobj):
    global features_to_use
    thisfeats = featurize.featurize_single_ts(tsobj,    features_to_use=features_to_use,
    raise_exceptions=False)
    return thisfeats

