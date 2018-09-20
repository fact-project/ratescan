#!/usr/bin/env python
import gzip
import pandas as pd
from fact.io import write_data
import click
import logging
from tqdm import tqdm

from gridmap import Job, process_jobs

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

def run(infile, keys):
    '''
    This is what will be executed on the cluster
    '''
    logger = logging.getLogger(__name__)
    logger.info("stream runner has been started.")

    open_func = open

    log.info("reading: {}".format(infile))
    if infile.endswith(".gz"):
        open_func = gzip.open

    dfs = []
    lines = 0
    with open_func(infile) as f:
        for line in f:
            df = pd.read_json(line)
            df = df[default_keys_to_store]
            dfs.append(df)
            lines+=1

    res = pd.concat(dfs)
    logger.info("extracted {} thresholds from {} lines".format(len(res), lines))
    return res

def make_jobs(infiles, keys, engine, queue, vmem, walltime, num_runs):
    jobs = []
    logger = logging.getLogger(__name__)
    logger.info("queue: {}".format(queue))
    logger.info("walltime: {}".format(walltime))
    logger.info("engine: {}".format(engine))
    logger.info("mem_free: {}mb".format(vmem))
    for num, infile in enumerate(infiles):
        jobs.append(
           Job(run,
               [infile, keys],
               queue=queue,
               walltime=walltime,
               engine=engine,
               name="{}_ratescan_convert".format(num),
               mem_free='{}mb'.format(vmem)
               )
           )
    return jobs

ratescan_keys = [
    'ratescan_trigger_counts',
    # 'ratescan_trigger_slices',
    # 'ratescan_trigger_primitives',
    'ratescan_trigger_thresholds'
]
meta_keys = [
    'event_num',
    'trigger_type',
    # 'num_pixel',
    'run_id',
    'night',
    # 'roi',
    'timestamp'
]
pointing_keys = [
    'source_position_zd', 'source_position_az', 'aux_pointing_position_zd', 'aux_pointing_position_az', 'pointing_position_zd', 'pointing_position_az'
]
pedestal_keys = [
    'ped_mean_median',
    # 'ped_mean_p25',
    # 'ped_mean_p75',
    'ped_mean_mean',
    # 'ped_mean_max',
    # 'ped_mean_min',
    'ped_var_median',
    # 'ped_var_p25',
    # 'ped_var_p75',
    'ped_var_mean',
    # 'ped_var_max',
    # 'ped_var_min',
    'ped_var_variance',
]

default_keys_to_store = ratescan_keys + meta_keys + pedestal_keys

@click.command()
@click.argument('infiles', nargs=-1, type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True) )
@click.argument('outfile', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
@click.option('--key', help='Key of the data base in the hdf file', default="ratescan")
@click.option('--queue', help='Name of the queue you want to send jobs to.', default='one_day')
@click.option('--walltime', help='Estimated maximum walltime of your job in format hh:mm:ss.', default='02:00:00')
@click.option('--engine', help='Name of the grid engine used by the cluster.', type=click.Choice(['PBS', 'SGE',]), default='PBS')
@click.option('--num_runs', help='Number of num runs per bunch to start on the cluster.', default='4', type=click.INT)
@click.option('--vmem', help='Amount of memory to use per node in MB.', default='10000', type=click.INT)
@click.option('--log_level', type=click.Choice(['INFO', 'DEBUG', 'WARN']), help='increase output verbosity', default='INFO')
@click.option("--log_dir", type=click.Path(exists=False, dir_okay=True, file_okay=False, readable=True), help='Directory to store output from m gridmap jobs', default=None)
@click.option('--port', help='The port through which to communicate with the JobMonitor', default=None, type=int)
@click.option('--local', default=False,is_flag=True,   help='Flag indicating whether jobs should be executed localy .')
def main(infiles, outfile, key, queue, walltime, engine, num_runs, vmem, log_level, log_dir, port, local):
    """
    run over list of jsonl files convert each line to pandas df and dump it to HDF5
    """

    log.info("Putting ratescans from json files into hdf5 file")
    open_func = open

    jobs = make_jobs(infiles, default_keys_to_store, engine, queue, vmem, walltime, num_runs)

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
        write_data(df, outfile, key=key, mode="a")

if __name__ == '__main__':
    main()
