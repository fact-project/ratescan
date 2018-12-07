#!/usr/bin/env python
import gzip
import pandas as pd
from fact.io import write_data, read_data
import click
import logging
import numpy as np
import os
from tqdm import tqdm

from gridmap import Job, process_jobs

from ..utils import *
from ..io import readJsonLtoDf
from ..features import *

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

default_key_dict = dict(
    inkey = "ratescan",
    night_key = "night",
    event_num_key = "event_num",
    run_id_key = "run_id",
    counts_key = "ratescan_trigger_counts",
    thresholds_key = "ratescan_trigger_thresholds",
    normalize=False,
    )

def run(
        infile_path, 
        key_dict=default_key_dict, 
        ):
    '''
    This is what will be executed on the cluster an will do the feater extraction from 
    ratescans
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")
    
    inkey = "ratescan" if "inkey" not in key_dict.keys() else key_dict["inkey"]
    night_key = "night" if "night_key" not in key_dict.keys() else key_dict["night_key"]
    event_num_key = "event_num" if "event_num_key" not in key_dict.keys() else key_dict["event_num_key"]
    run_id_key = "run_id" if "run_id" not in key_dict.keys() else key_dict["run_id"]
    counts_key = "ratescan_trigger_counts" if "counts_key" not in key_dict.keys() else key_dict["counts_key"]
    thresholds_key = "ratescan_trigger_thresholds" if "thresholds_key" not in key_dict.keys() else key_dict["thresholds_key"]
    
    df = None
    
    relevant_keys= [night_key, event_num_key, run_id_key, counts_key, thresholds_key]
    
    if infile_path.endswith("json.gz") or infile_path.endswith("json"):
        df = readJsonLtoDf(infile_path, default_keys_to_store=relevant_keys)
    elif infile_path.endswith("hdf") or infile_path.endswith("hdf5"):
        df = read_data(infile_path, key=inkey)
    else:
        logger.error("input fileformat not supported")
        df = None
        
    # at this stage we expect an data frame with coulmns containing:
    # thresholds, trigger_counts, event_ids, night_id (optional), run_id (optional)
    logger.info("Get all single event features from input file")
    # df_features = df[df[thresholds_key] == df[thresholds_key].min()]
    
    logger.info("Extracting feature: maxPossibleThreshold2Keep")
    df_thresholds = maxPossibleThreshold2Keep(
            df, 
            counts_key=counts_key,
            thresholds_key=thresholds_key,
            event_num_key=event_num_key,
            night_key=night_key,
            run_id_key=run_id_key,
        )
    
    logger.info("Converting to rates")
    df_ratescans = sumUpAndConvertToRates(
                        df,
                        night_key=night_key, 
                        run_id_key=run_id_key,
                        counts_key=counts_key, 
                        thresholds_key=thresholds_key,
                        rates_key="ratescan_trigger_rates",
                        normalize=key_dict['normalize'],
                        )
        
    logger.info("Extracting feature: ratescanTriggerSetThreshold")
    ss = []
    
    for k, ((night, run_id), group) in enumerate(df_ratescans.groupby([night_key, run_id_key])):
        s_fit_results = findTriggerSetThreshold(
            group,
            rate_key = "ratescan_trigger_rates", 
            thresholds_key=thresholds_key,
            )
        s_fit_results[night_key] = night
        s_fit_results[run_id_key] = run_id

        ss.append(s_fit_results)

    df_ratescan_fits = pd.concat(ss, axis=1).T
    
    df_result = pd.merge(df_thresholds, df_ratescan_fits, how="inner", on=[night_key, run_id_key])
    
    df_result["infile_path"] = infile_path
    
    return df_result


def make_jobs(infiles, key_dict, engine, queue, vmem, walltime):
    jobs = []
    logger = logging.getLogger(__name__)
    logger.info("queue: {}".format(queue))
    logger.info("walltime: {}".format(walltime))
    logger.info("engine: {}".format(engine))
    logger.info("mem_free: {}mb".format(vmem))
    for num, infile in enumerate(infiles):
        jobs.append(
           Job(run,
               [infile, key_dict],
               queue=queue,
               walltime=walltime,
               engine=engine,
               name="{}_ratescan_feature_extract".format(num),
               mem_free='{}mb'.format(vmem)
               )
           )
    return jobs



@click.command()
@click.argument('infiles', nargs=-1, type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('outfile', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--outkey', help='Key of the data base in the hdf file', default="ratescan")
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='one_day')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='PBS')
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--chunksize', help='number of simultaneus submitted jobs.', default='0', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option("--log_dir", type=click.Path(exists=False, dir_okay=True, file_okay=False, readable=True), help='Directory to store output from m gridmap jobs', default=None)
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=None, type=int)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.option('--mc', default=False, is_flag=True,   help='Flag indicating whether input files are mcs.')
def main(infiles, outfile, outkey, queue, walltime, engine, vmem, chunksize, log_level, log_dir, port, local, mc):
    """
    run over list of jsonl files convert each line to pandas df and dump it to HDF5
    """

    log.info("Putting ratescans from json files into hdf5 file")
    open_func = open
    
    if chunksize > 0:
        partitions = np.array_split(infiles, 1+len(infiles)//chunksize)
    else:
        partitions = np.array_split(infiles, 1)

    if mc:
        default_key_dict['night_key']  = "lons_night"
        default_key_dict['run_id_key'] = "lons_run_id"
        default_key_dict['normalize'] = True

    for infile in partitions:
        jobs = make_jobs(infile, default_key_dict, engine, queue, vmem, walltime)

        log.info("Submitting {} jobs".format(len(jobs)))

        job_arguments = dict(
            jobs=jobs,
            max_processes=len(jobs),
            local=local,
        )

        if port:
            job_arguments["port"] = port

        if log_dir:
            job_arguments["temp_dir"] = log_dir

        job_outputs = process_jobs(**job_arguments)

        for df in tqdm(job_outputs):
            write_data(df, outfile, key=outkey, mode="a")


if __name__ == '__main__':
    main()
