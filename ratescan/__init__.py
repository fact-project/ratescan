import gzip
import pandas as pd
import logging

def jsonToDf(infile, keys):
    '''
    Covert a json file to a data frame
    '''
    log = logging.getLogger(__name__)
    
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