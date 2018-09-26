from fact.io import read_data

def test_sumupCountsOfRun():
    from ratescan.utils import sumupCountsOfRun
    
    df = read_data("test/test.hdf5", key="ratescan")
    
    df_summed = sumupCountsOfRun(df)
    
    assert df_summed.run_id.unique() == 182
    assert len(df_summed.ratescan_trigger_thresholds) == 1000

def test_compileRatescanForRun():
    from ratescan.utils import compileRatescanForRun
    
    df = read_data("test/test.hdf5", key="ratescan")
    
    df = compileRatescanForRun(df, ontime=160)
    
    assert df[df["ratescan_trigger_counts"] == 75840]["ratescan_trigger_rate"].unique() == 474.0
# 
# def test_joinOnTimesFromRunDB():  
#     from ratescan.utils import joinOnTimesFromRunDB
# 
#     df = read_data("test/test.hdf5", key="ratescan")
# 
#     df_res = joinOnTimesFromRunDB(df)
