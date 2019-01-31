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

from ..utils import *
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
    'lons_run_id', 'lons_night', 'lons_event_num'
]

default_obs_cols =[
    "night",
    ]



def run(infile_path, inkey=None, key_list=None):
    '''
    This is what will be executed on the cluster an will do extraction of
    ratescans
    :param inkey:
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    if inkey is None:
        inkey = "ratescan"

    if key_list is None:
        key_list = default_common_cols + default_obs_cols
    
    if infile_path.endswith("json.gz") or infile_path.endswith("json"):
        df = readJsonLtoDf(infile_path, default_keys_to_store=key_list)
    elif infile_path.endswith("hdf") or infile_path.endswith("hdf5"):
        df = read_data(infile_path, key=inkey)
    else:
        logger.error("input fileformat not supported")
        df = None

    df["infile_path"] = infile_path
    
    return df


def make_jobs(infiles, engine, queue, vmem, walltime, key_list=None, inkey=None):
    jobs = []
    logger = logging.getLogger(__name__)
    logger.info("queue: {}".format(queue))
    logger.info("walltime: {}".format(walltime))
    logger.info("engine: {}".format(engine))
    logger.info("mem_free: {}mb".format(vmem))
    for num, infile in enumerate(infiles):
        jobs.append(
           Job(run,
               [infile, inkey, key_list],
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

    key_list = default_common_cols
    if mc:
        key_list += default_sim_cols
    else:
        key_list += default_obs_cols

    for infile in partitions:
        jobs = make_jobs(infile, engine, queue, vmem, walltime, key_list)

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

        for k, df in tqdm(enumerate(job_outputs)):
            mode = 'w' if k < 1 else "a"
            write_data(df, outfile, key=outkey, mode=mode, index=False)
            del df
            gc.collect()

        del job_outputs
        gc.collect()


if __name__ == '__main__':
    main()
