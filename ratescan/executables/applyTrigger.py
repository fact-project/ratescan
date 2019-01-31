#!/usr/bin/env python
import gzip
import pandas as pd
from fact.io import write_data, read_data
import h5py
import click
import logging
import numpy as np
import os
from tqdm import tqdm

from gridmap import Job, process_jobs

from ..utils import *
from ..io import readJsonLtoDf
from ..features import *


log = logging.getLogger(__name__)

logLevel = dict()
logLevel["INFO"] = logging.INFO
logLevel["DEBUG"] = logging.DEBUG
logLevel["WARN"] = logging.WARN

mc_id_keys = [
    'corsika_event_header_event_number',
    'corsika_run_header_run_number',
    'event_num',
    'run_id',
]

data_id_keys = [
    'event_num',
    'run_id',
    'night'
]


def run(
        infile_group, df_trigger, id_keys, input_dataset_path
        ):
    '''
    This is what will be executed on the cluster an will do extraction of
    ratescans
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    df_data = read_data(input_dataset_path, key=infile_group, mode='r')
    df_merge = pd.merge(df_data, df_trigger, on=id_keys, indicator=True)
    df_merge = df_merge[df_merge['_merge'] == 'both']
    df_merge = df_merge[df_data.columns]
    log.debug(
        f'trigger reduction in group {infile_group}: {len(df_data)} before, {len(df_merge)} after, {100*(1 - len(df_merge)/len(df_data)):.3f}% loss')
    return infile_group, df_merge


def make_jobs(infile_groups, df_trigger, id_keys, input_dataset_path, engine, queue, vmem, walltime):
    jobs = []
    logger = logging.getLogger(__name__)
    logger.info("queue: {}".format(queue))
    logger.info("walltime: {}".format(walltime))
    logger.info("engine: {}".format(engine))
    logger.info("mem_free: {}mb".format(vmem))
    for num, infile_group in enumerate(infile_groups):
        jobs.append(
           Job(run,
               [infile_group, df_trigger, id_keys, input_dataset_path],
               queue=queue,
               walltime=walltime,
               engine=engine,
               name="{}_ratescan_apply_trigger".format(num),
               mem_free='{}mb'.format(vmem)
               )
           )
    return jobs



@click.command()
@click.argument('input_dataset_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('trigger_decision_path', type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('output_path', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--trigger_group_key', help='Key of the data base in the hdf file', default="ratescan")
@click.option('--trigger_threshold_key', help='Key of the data base in the hdf file', default="SetSWThreshold_41.2_0.551")
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option('--ismc', is_flag=True)
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='short')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='00:30:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='PBS')
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option("--log_dir", type=click.Path(exists=False, dir_okay=True, file_okay=False, readable=True), help='Directory to store output from m gridmap jobs', default=None)
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=None, type=int)
@click.option('--local', default=False, is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
def main(input_dataset_path, trigger_decision_path, output_path,
         trigger_group_key, trigger_threshold_key, log_level, ismc,
         queue, walltime, engine, vmem, log_dir, port, local):
    """
    apply the software trigger to the input data set
    """

    logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logLevel[log_level])

    log.info("apply the software trigger to the input data set")


    log.info("Reading groups from input file")
    with h5py.File(input_dataset_path, "r") as f:
        infile_groups = list(f.keys())

    log.info("Reading trigger decission file")
    df_trigger = read_data(trigger_decision_path, key=trigger_group_key)
    df_trigger = df_trigger[df_trigger[trigger_threshold_key]]

    if ismc:
        log.info("MC mode")
        id_keys = mc_id_keys
    else:
        log.info("Data mode")
        id_keys = data_id_keys

    log.info(f"Applying trigger to groups in data set file")

    jobs = make_jobs(infile_groups, df_trigger, id_keys, input_dataset_path, engine, queue, vmem, walltime)

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

    for k, (infile_group, df_merge) in tqdm(enumerate(job_outputs)):
        mode = 'w' if k < 1 else "a"
        write_data(df_merge, output_path, key=infile_group, mode=mode, index=False)


if __name__ == '__main__':
    main()
