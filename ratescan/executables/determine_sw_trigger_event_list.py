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
import gc

from ..utils import append_current_at_start_from_run_db
from ..io import readJsonLtoDf
from ..features import *

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

default_common_cols = [
    "event_num",
    "run_id",
    "ratescan_trigger_counts",
    "ratescan_trigger_thresholds",
]

default_sim_cols = [
    'corsika_event_header_event_number',
    'corsika_run_header_run_number',
    'run_id',
    'lons_run_id', 'lons_night', 'lons_event_num'
]

default_obs_cols =[
    "night",
    ]

default_key_dict = dict(
    inkey="ratescan",
    night_key="night",
    event_num_key="event_num",
    run_id_key="run_id",
    keys_to_read=default_common_cols + default_obs_cols,
    counts_key="ratescan_trigger_counts",
    thresholds_key="ratescan_trigger_thresholds",
    threshold_curve_par=[{'a': 63.2, 'k': 0.551 }],
    normalize=False,
    group_keys=["night", "run_id", "event_num"],
)


def get_value_from_dict_or_use_default(key_dict, key):
    return key_dict[key] if key in key_dict.keys() else default_key_dict[key]


def run(infile_path, inkey=None, key_dict=None):
    '''
    This is what will be executed on the cluster an will do extraction of
    ratescans
    :param inkey:
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    if inkey is None:
        inkey = "ratescan"

    if key_dict is None:
        key_dict = default_key_dict

    night_key = get_value_from_dict_or_use_default(key_dict, "night_key")
    run_id_key = get_value_from_dict_or_use_default(key_dict, "run_id_key")
    group_keys = get_value_from_dict_or_use_default(key_dict, "group_keys")
    counts_key = get_value_from_dict_or_use_default(key_dict, "counts_key")
    thresholds_key = get_value_from_dict_or_use_default(key_dict, "thresholds_key")
    threshold_curve_par = get_value_from_dict_or_use_default(key_dict, "threshold_curve_par")
    keys_to_read = get_value_from_dict_or_use_default(key_dict, "keys_to_read")
    
    if infile_path.endswith("json.gz") or infile_path.endswith("json"):
        df = readJsonLtoDf(infile_path, default_keys_to_store=keys_to_read)
    elif infile_path.endswith("hdf") or infile_path.endswith("hdf5"):
        df = read_data(infile_path, key=inkey)
    else:
        logger.error("input fileformat not supported")
        df = None


    df["infile_path"] = infile_path

    df = append_current_at_start_from_run_db(df,
                                             night_key=night_key,
                                             run_id_key=run_id_key,
                                             current_key='current_at_start'
                                             )

    df_trigger = []
    for parameter_pair in threshold_curve_par:
        a = parameter_pair['a']
        k = parameter_pair['k']
        threshold_name = f'SetSWThreshold_{a}_{k}'
        df[threshold_name] = df.apply(lambda row: power_law(row['current_at_start'], a, k), axis=1)

        def has_triggered(group):
            above_threshold = group[group[thresholds_key] >= group[threshold_name]]
            if len(above_threshold) > 0:
                counts_above_threshold = above_threshold[above_threshold[counts_key] > 0]
                if len(counts_above_threshold) > 0:
                    return True
            return False

        print(df.columns)
        df_trigger_decission = df.groupby(group_keys).apply(has_triggered)
        df_trigger_decission.name = threshold_name
        df_trigger.append(df_trigger_decission)

    if len(df_trigger) == 1:
        return df_trigger[0].reset_index()
    else:
        df_trigger = pd.concat(df_trigger, axis=1)
        return df_trigger.reset_index()


def power_law(x, a, k):
    return a * pow(x, k)


def make_jobs(infiles, engine, queue, vmem, walltime, key_dict=None, inkey=None):
    jobs = []
    logger = logging.getLogger(__name__)
    logger.info("queue: {}".format(queue))
    logger.info("walltime: {}".format(walltime))
    logger.info("engine: {}".format(engine))
    logger.info("mem_free: {}mb".format(vmem))
    for num, infile in enumerate(infiles):
        jobs.append(
           Job(run,
               [infile, inkey, key_dict],
               queue=queue,
               walltime=walltime,
               engine=engine,
               name="{}_ratescan_concat".format(num),
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
@click.option('--local', default=False, is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
@click.option('--mc', default=False, is_flag=True,   help='Flag indicating whether input files are mcs.')
@click.option('-t', '--threshold_curve_par', nargs=2, type=click.Tuple([float, float]), multiple=True, default=(63.2, 0.551))
def main(infiles, outfile, outkey, queue, walltime, engine, vmem, chunksize, log_level, log_dir, port, local, mc, threshold_curve_par):
    """
    run over list of jsonl files convert each line to pandas df and dump it to HDF5
    """

    log.info("Putting ratescans from json files into hdf5 file")

    if chunksize > 0:
        partitions = np.array_split(infiles, 1+len(infiles)//chunksize)
    else:
        partitions = np.array_split(infiles, 1)

    key_dict = default_key_dict
    key_dict['threshold_curve_par'] = []
    for (a,k) in threshold_curve_par:
        key_dict['threshold_curve_par'].append(dict(a=a, k=k))

    if mc:
        key_dict['night_key'] = 'lons_night'
        key_dict['run_id_key'] = 'lons_run_id'
        key_dict['keys_to_read'] = default_common_cols + default_sim_cols
        key_dict['group_keys'] = ["corsika_event_header_event_number", "corsika_run_header_run_number", "run_id", "event_num"]
    else:
        key_dict['keys_to_read'] = default_common_cols + default_obs_cols
        key_dict['group_keys'] = ["night", "run_id", "event_num"]

    for infile in partitions:
        jobs = make_jobs(infile, engine, queue, vmem, walltime, key_dict)

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

        df = pd.concat(job_outputs)
        write_data(df, outfile, key=outkey, mode='w', index=False)


if __name__ == '__main__':
    main()
