#!/usr/bin/env python
import gzip
import pandas as pd
from fact.io import write_data
import click
import logging
from tqdm import tqdm   

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

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
def main(infiles, outfile, key):
    """
    run over list of jsonl files convert each line to pandas df and dump it to HDF5
    """
    
    log.info("Putting ratescans from json files into hdf5 file")
    open_func = open
    
    for infile in tqdm(infiles):
        log.info("reading: {}".format(infile))
        if infile.endswith(".gz"):
            open_func = gzip.open
            
        with open_func(infile) as f:
            for line in tqdm(f):
                df = pd.read_json(line)
                df = df[default_keys_to_store]
                write_data(df, outfile, key=key, mode="a")

if __name__ == '__main__':
    main()