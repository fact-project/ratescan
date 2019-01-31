import gzip
import pandas as pd
import logging
import json

logging.basicConfig(format='%(asctime)s|%(levelname)s|%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
log = logging.getLogger(__name__)

def readJsonLtoDf(infile_path, default_keys_to_store=None):
    open_func = open

    log.info("reading: {}".format(infile_path))
    if infile_path.endswith(".gz"):
        open_func = gzip.open

    dfs = []
    # lines = 0
    with open_func(infile_path) as f:
        for line in f:
            data = json.loads(line)
            for k in default_keys_to_store:
                if k not in data.keys():
                    log.warning((f'{k} not in keys of input file'))

            data = {k: data[k] for k in default_keys_to_store if k in data.keys()}

            df = pd.DataFrame(data)

            # df = pd.read_json(line)
            # if len(default_keys_to_store) > 0:
                # df = df[default_keys_to_store]
            dfs.append(df)
            # lines+=1
    
    return pd.concat(dfs)