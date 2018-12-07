from fact.factdb import RunInfo
from fact.factdb.utils import read_into_dataframe
from peewee import SQL, fn
import pandas as pd

def sumupCountsOfRun(
        df, 
        group_keys=["night", "run_id", "ratescan_trigger_thresholds"],
        counts_key="ratescan_trigger_counts", 
        ):
    
    df_summed = df[[counts_key, *group_keys]].groupby(group_keys).sum()
    df_summed = df_summed.reset_index()

    df_size = df[[counts_key, *group_keys]].groupby(group_keys).size()
    df_size = df_size.to_frame(name='n_events_per_run')
    df_size = df_size.reset_index()

    return pd.merge(df_summed, df_size, how='left', on=group_keys, suffixes=['', '_nevents'])


def compileRatescanForRun(
        df,
        ontime=None,
        rate_key = "ratescan_trigger_rate",
        counts_key = "ratescan_trigger_counts",
        thresholds_key = "ratescan_trigger_thresholds",
        ):
    """
    Sum all rate counts for all thresholds for a given run. If a ontime is given
    convert counts to rates
    """
    df_new = sumupCountsOfRun(
        df,
        group_keys=[thresholds_key],
        counts_key=counts_key
    )
    
    if ontime:
        df_new[rate_key] = df_new[counts_key]/ontime
 
    return df_new


def joinOnTimesFromRunDB(df,
                        night_key="night", 
                        run_id_key="run_id",
                        ):
    query = (
        RunInfo.select(
            RunInfo.fnight.alias('night'),
            RunInfo.frunid.alias('run_id'),
            (
                fn.TIMESTAMPDIFF(SQL('SECOND'), 
                RunInfo.frunstart, 
                RunInfo.frunstop
            )*RunInfo.feffectiveon).alias('ontime'),
        )
        .where(RunInfo.fnight >= df[night_key].min())
        .where(RunInfo.fnight <= df[night_key].max())
        .where(RunInfo.frunid >= df[run_id_key].min())
        .where(RunInfo.frunid <= df[run_id_key].max())
    )
    
    df_run_db = read_into_dataframe(query)
    
    return pd.merge(df, df_run_db, how='left', left_on=[night_key, run_id_key], right_on=['night', 'run_id'])


def sumUpAndConvertToRates(
                        df,
                        night_key="night", 
                        run_id_key="run_id",
                        counts_key="ratescan_trigger_counts", 
                        thresholds_key="ratescan_trigger_thresholds",
                        rates_key="ratescan_trigger_rates",
                        ):
                        
    df_result = sumupCountsOfRun(
                    df, 
                    group_keys=[night_key, run_id_key, thresholds_key],
                    counts_key=counts_key, 
                    )

    df_result = joinOnTimesFromRunDB(
                        df_result,
                        night_key=night_key, 
                        run_id_key=run_id_key,
                        )
    
    df_result[rates_key] = df_result[counts_key]/df_result['ontime']
    return df_result