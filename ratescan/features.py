import numpy as np
import pandas as pd
import math as m
from scipy.optimize import root, curve_fit

from .models import *

def maxPossibleThreshold2Keep(
        df,
        n_primitives = 1,
        counts_key = "ratescan_trigger_counts",
        thresholds_key = "ratescan_trigger_thresholds",
        event_num_key = "event_num",
        night_key = "night",
        run_id_key = "run_id",
        ):
    """
    For a given dataframe with with columns event_keys, counts_key and 
    thresholds_key get per night and run_id the value of the highest possilble 
    trigger threshold to trigger n_primitives,
    """
    event_keys = [event_num_key, run_id_key, night_key]
    
    # Get all events with one triggering patch
    df_tmp = df[df[counts_key] == n_primitives]
    
    groups = df_tmp[[*event_keys, thresholds_key]].groupby(event_keys)
    df_tmp = groups.max()
    df_tmp.reset_index(inplace=True)
    
    # now we havethe max possible threshold to keep for each event. 
    # Next step is to find the event with the smallest possible threshold.
    group_keys = None
    if all(key in event_keys for key in [night_key, run_id_key]):
        group_keys = [night_key, run_id_key]
    elif run_id_key in event_keys:
        group_keys = run_id_key
    
    if group_keys:
        df_tmp = df_tmp.groupby(group_keys).min()
        df_tmp.reset_index(inplace=True)
        return df_tmp
    else:
        return df_tmp.min()
        

def fit_given_range(df_ranged, thresholds_key, rate_key, func, p0=None):
    xdata = df_ranged[thresholds_key].values
    ydata = df_ranged[rate_key].values
        
    return curve_fit(func, xdata, ydata, p0=p0)
        
def findTriggerSetThreshold(
        df,
        max_threshold = 5000,
        scale=1,
        rate_key = "ratescan_trigger_rate",
        thresholds_key = "ratescan_trigger_thresholds",
        ):
    """
    Find the threshold where the shower contribution of the triggerate is
    equal to 1/e of the NSB contribution for a given ratescan
    """
    max_rate = df[rate_key].max()
    
    ranges_dict = dict()
    
    ranges_dict["max_rate"] = max_rate
        
    ranges_dict["max_threshold"] = max_threshold
    ranges_dict["nsb_rate_max"] = max_rate*0.6
    ranges_dict["nsb_rate_min"] = max_rate*0.1
    ranges_dict["shower_rate_max"] = ranges_dict["nsb_rate_min"]/2
    
    filter_shower_thresh_min = df[rate_key] < ranges_dict["shower_rate_max"]
    filter_shower_thresh_max = df[thresholds_key] < ranges_dict["max_threshold"]
    filter_nsb_thresh_min = df[rate_key] < ranges_dict["nsb_rate_max"]
    filter_nsb_thresh_max = df[rate_key] > ranges_dict["nsb_rate_min"]
    
    s_fit_results = []
    s_fit_results.append(pd.Series(ranges_dict, name="ranges"))
    
    filter_shower_range = np.logical_and(filter_shower_thresh_min, filter_shower_thresh_max)
    filter_nsb_range = np.logical_and(filter_nsb_thresh_min, filter_nsb_thresh_max)
    filter_full_range = np.logical_and(filter_nsb_thresh_min, filter_shower_thresh_max)
    
    
    
    shower_opt, shower_cov = fit_given_range(
        df[filter_shower_range], 
        thresholds_key, 
        rate_key, 
        powerLaw,
        p0=[5.52310784e10, -3, 3.70127911e2])
    fit_results["shower"] = dict( opt=shower_opt, cov=shower_cov)
     
    nsb_opt, nsb_cov = fit_given_range(
        df[filter_nsb_range], 
        thresholds_key, 
        rate_key, 
        nsbContribution,
        p0=[-2.18757227e-2, 6.94879358e2])
    fit_results["nsb"] = dict( opt=nsb_opt, cov=nsb_cov)
        
    full_opt, full_cov = fit_given_range(
        df[filter_full_range], 
        thresholds_key, 
        rate_key, 
        ratescan_func,
        p0=[*shower_opt, *nsb_opt])
    fit_results["full"] = dict( opt=full_opt, cov=full_cov)
    
    #estimate location
    estimatedThreshold = df[df[rate_key] <= max_rate*0.1][thresholds_key].dropna().first_valid_index()
    
    poly = lambda t: powerLaw(t, *full_opt[:3])/(m.e*scale) - nsbContribution(t, *full_opt[3:])
    solution = root(poly, estimatedThreshold)
    
    return solution.x, fit_results
    
    
    
    
    
    
