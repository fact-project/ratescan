#!/usr/bin/env python
import pandas as pd
from fact.io import read_data
import click
import logging

import matplotlib.pyplot as plt

from fact.credentials import create_factdb_engine
from fact.factdb import *
from fact.factdb.utils import read_into_dataframe
from peewee import SQL, fn

from ..features import findTriggerSetThreshold
from ..models import *

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

@click.command()
@click.argument('infile', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('outfile', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--key', help='Key of the data base in the hdf file', default="ratescan")

@click.option('--run_id', default=182)
@click.option('--night', default=20150901)
@click.option('--run_id_key', default="run_id")
@click.option('--night_key', default="night")
@click.option('--counts_key', default="ratescan_trigger_counts")
@click.option('--thresholds_key', default="ratescan_trigger_thresholds")
# @click.option('--queue', help='Name of the queue you want to send jobs to.', default='one_day')
# @click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
# @click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='PBS')
# @click.option('--num_runs', help='Number of num runs per bunch to start on the cluster.', default='4', type=click.INT)
# @click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
# @click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
# @click.option("--log_dir", type=click.Path(exists=False, dir_okay=True, file_okay=False, readable=True), help='Directory to store output from m gridmap jobs', default=None)
# @click.option('--port', help='The port through which to communicate with the JobMonitor', default=None, type=int)
# @click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
def main(infile, outfile, key, run_id, night, run_id_key, night_key, counts_key, thresholds_key):
    """
    Extract a single ratescan from a DataFrame, fit the model and plot ist
    """
    
    log.info("reading {}".format(infile))
    df = read_data(infile, key=key)
            
    if len(df[df[night_key] == night]) == 0:
        night = df[night_key].unique()[0]
        
    df = df[df[night_key] == night]
    
    
    if len(df[df[run_id_key] == run_id]) == 0:
        run_id = df[run_id_key].unique()[0]
    
    df = df[df[run_id_key] == run_id]
        
    df = df[[
        counts_key,
        thresholds_key,
        run_id_key,
        night_key
        ]]
    
    query = (
        RunInfo.select(
    #         RunInfo,
            RunInfo.fnight.alias('night'),
            RunInfo.frunid.alias('run_id'),
            (fn.TIMESTAMPDIFF(SQL('SECOND'), RunInfo.frunstart, RunInfo.frunstop) * RunInfo.feffectiveon).alias('ontime'),
        )
        .join(Source, on=Source.fsourcekey==RunInfo.fsourcekey)
        .where(RunInfo.fnight == df[night_key].unique())
        .where(RunInfo.frunid == df[run_id_key].unique())
    )
    log.info("reading data for night {}, run_id {} from run DB".format(night, run_id))
    df_run_db = read_into_dataframe(query)
    ontime = df_run_db.ontime.unique()
    
    log.info("Sumup events")
    df_summed = df[[counts_key, thresholds_key]].groupby(thresholds_key).sum()
    
    df_summed["Rates"] = df_summed[counts_key]/ontime
    
    df_summed = df_summed.reset_index()
    
    log.info("Fitting")
    sol, fr = findTriggerSetThreshold(df=df_summed, rate_key = "Rates")
    
    log.info("Trigger Setpoint found for: {}".format(sol))
    
    log.info("Plotting")
    fig = plt.figure()
    
    xdata = np.linspace(0, 2000, 2000)
    
    # df.plot(logy=True)
    
    plt.plot(df_summed[thresholds_key].values, df_summed["Rates"].values, zorder=1)
    
    plt.plot(xdata, ratescan_func(xdata, *fr["full"]["opt"]), 'C1--', label="full fit")
    plt.plot(xdata, nsbContribution(xdata, *fr["full"]["opt"][3:]), 'C2-.', label="nsb_full")
    plt.plot(xdata, powerLaw(xdata, *fr["full"]["opt"][:3]), 'C2--', label="shower_full")
    plt.plot(xdata, nsbContribution(xdata, *fr["nsb"]["opt"]), 'C3--', label="nsb fit")
    plt.plot(xdata, powerLaw(xdata, *fr["shower"]["opt"]), 'C4--', label="shower fit")
    plt.semilogy()
    plt.xlim(0,2000)
    plt.ylim(1e0,np.max(df_summed["Rates"].values)*10)
    
    # plt.axvline(sol)
    plt.legend()
    
    
    plt.axhline(fr["shower_rate_max"], label= "shower_rate_max", c="k", alpha=0.5, ls="--", lw=0.8)
    plt.axhline(fr["nsb_rate_min"], label="nsb_rate_min", c="k", alpha=0.5, ls="--", lw=0.8)
    plt.axhline(fr["nsb_rate_max"], label="nsb_rate_max", c="k", alpha=0.5, ls="--", lw=0.8)
    fig.savefig(outfile)

if __name__ == '__main__':
    main()
