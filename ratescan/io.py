import gzip
import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

def readJsonLtoDf(infile_path, default_keys_to_store=None):
    open_func = open

    log.info("reading: {}".format(infile_path))
    if infile_path.endswith(".gz"):
        open_func = gzip.open

    dfs = []
    lines = 0
    with open_func(infile_path) as f:
        for line in f:
            df = pd.read_json(line)
            if len(default_keys_to_store) > 0:
                df = df[default_keys_to_store]
            dfs.append(df)
            lines+=1
    
    return pd.concat(dfs)