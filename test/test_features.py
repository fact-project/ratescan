from fact.io import read_data
import numpy as np

def test_maxPossibleThreshold2Keep():
    from ratescan.features import maxPossibleThreshold2Keep
    
    df = read_data("test/test.hdf5", key="ratescan")
    
    df_thresholds = maxPossibleThreshold2Keep(df)

    assert df_thresholds["ratescan_trigger_thresholds"].values == 490
    assert len(df_thresholds["ratescan_trigger_thresholds"]) == 1
    # from IPython import embed; embed()
    
    
def test_findTriggerSetThreshold():
    from ratescan.features import findTriggerSetThreshold
    from ratescan.utils import compileRatescanForRun
    
    df = read_data("test/test.hdf5", key="ratescan")
    
    df = compileRatescanForRun(df, ontime=160)
    
    df_thresholds = findTriggerSetThreshold(df, max_threshold = 5000)
    
    assert np.around(df_thresholds["setThreshold"],1) == 500.9
    assert df_thresholds["ranges_max_rate"] == 474
    assert np.around(df_thresholds["ranges_shower_rate_max"],1) == 23.7
    assert np.around(df_thresholds["ranges_nsb_rate_min"],1) == 47.4
    assert np.around(df_thresholds["ranges_nsb_rate_max"],1) == 284.4
    assert df_thresholds["ranges_max_threshold"] == 5000
