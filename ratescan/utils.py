from fact.factdb import RunInfo
from fact.factdb.utils import read_into_dataframe
from peewee import SQL, fn
import pandas as pd
from random import randint
from time import sleep

def sumupCountsOfRun(
        df,
        group_keys=["night", "run_id"],
        counts_key="ratescan_trigger_counts",
        thresholds_key="ratescan_trigger_thresholds"
):
    
    df_summed = df[[counts_key, thresholds_key, *group_keys]].groupby(group_keys+[thresholds_key]).sum()
    df_summed = df_summed.reset_index()

    df_zero = df[df[thresholds_key] == 0][[counts_key, *group_keys]]
    df_size = df_zero[[counts_key, *group_keys]].groupby(group_keys).size()
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
    df_new = sumupCountsOfRun(df, thresholds_key=thresholds_key, counts_key=counts_key)
    
    if ontime:
        df_new[rate_key] = df_new[counts_key]/ontime
 
    return df_new


def append_current_at_start_from_run_db(df,
                                        night_key="night",
                                        run_id_key="run_id",
                                        current_key='current_at_start',
                                        ):
    query = (
        RunInfo.select(
            RunInfo.fnight.alias('night'),
            RunInfo.frunid.alias('run_id'),
            RunInfo.fcurrentsmedmeanbeg.alias(current_key)
        )
            .where(RunInfo.fnight >= df[night_key].min())
            .where(RunInfo.fnight <= df[night_key].max())
            .where(RunInfo.frunid >= df[run_id_key].min())
            .where(RunInfo.frunid <= df[run_id_key].max())
    )

    # This necessary so the FACT DB does not go bananas
    sleep(randint(10, 5 * 60))

    df_run_db = read_into_dataframe(query)

    return pd.merge(df, df_run_db, how='left', left_on=[night_key, run_id_key], right_on=['night', 'run_id'],
                    suffixes=('', '_run_info'))


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

    # This necessary so the FACT DB does not go bananas
    sleep(randint(10, 5*60))

    df_run_db = read_into_dataframe(query)
    
    return pd.merge(df, df_run_db, how='left', left_on=[night_key, run_id_key], right_on=['night', 'run_id'])


def sumUpAndConvertToRates(
                        df,
                        night_key="night", 
                        run_id_key="run_id",
                        counts_key="ratescan_trigger_counts", 
                        thresholds_key="ratescan_trigger_thresholds",
                        rates_key="ratescan_trigger_rates",
                        normalize=False
                        ):
                        
    df_result = sumupCountsOfRun(df, group_keys=[night_key, run_id_key],
                                 thresholds_key=thresholds_key, counts_key=counts_key)

    df_result = joinOnTimesFromRunDB(
                        df_result,
                        night_key=night_key, 
                        run_id_key=run_id_key,
                        )
    
    df_result[rates_key] = df_result[counts_key]
    if not normalize:
        df_result[rates_key] /= df_result['ontime']
    else:
        df_result[rates_key] /= df_result['n_events_per_run']
    return df_result


def to_numeric_if_possible(val):
    try:
        return pd.to_numeric(val)
    except ValueError:
        return val