import sys
import os, errno
import glob
import ruffus as rf
import ruffus.cmdline as cmdline
import pandas as pd
# import datetime as dt
import numpy as np
# import math
# from sklearn import preprocessing
# import sklearn.metrics
import os, errno
import yaml
# import warnings
# warnings.filterwarnings("ignore", message=".*deprecation.*")

# Configuration and command line options
parser = cmdline.get_argparse(description='Pipeline using the CRIME example.')
parser.add_argument("--config")
options = parser.parse_args()
lg, lm = cmdline.setup_logging (__name__, options.log_file, options.verbose)
if vars(options)['config'] == None:
    print "No config supplied."
    parser.print_help()
    sys.exit()
with open(vars(options)['config'], 'r') as f: cfg = yaml.load(f)
lg.info('pipeline:: ::config %s'%str(cfg))

# =============================================================
#                    pow, 1st
# =============================================================

pipeline_dir = os.path.join(cfg['pipelines'],cfg['name'])
transform_dir = os.path.join(pipeline_dir, cfg['transform']['pow']['1st']['dir'])

# Input query file paths
path_glob = os.path.join(transform_dir, 'q', '*.yaml')
q_paths = glob.glob(path_glob)

# Input pairs the exponents list with each query file
exp_path = os.path.join(pipeline_dir, cfg['transform']['pow']['exponents'])
in_paths = [[q_path, exp_path] for q_path in q_paths]

@rf.mkdir(in_paths, rf.formatter(), "{path[0]}/{basename[0]}")
@rf.transform(in_paths, rf.formatter(),
              "{path[0]}/{basename[0]}/{basename[0]}.h5", lg, lm)
def pow1st(in_path, out_path, lg, lm):
    """Load inputs and prepare transformed instances."""

    q_path,exp_path = in_path   # query file and file listing exponents
    lg.info("xform::pow1st ::out_path %s"%out_path)
    lg.info("xform::pow1st ::q_file %s"%q_path)
    lg.info("xform::pow1st ::exponents_file %s"%exp_path)
    
    exponents = pd.read_csv(exp_path, comment="#")
    exponents['exponent'] = 1.*exponents.numerator/exponents.denominator

    with open(q_path,'r') as f: qcfg = yaml.load(f) # query config

    ous = pd.HDFStore(out_path,'w') # To hold instances, some xformed
    try:
        # Store the non-transformed instance
        for rel,sql in qcfg['relations'].iteritems():
            lg.info("xform::pow1st ::relation %s"%rel)
            k = 'orig/'+rel # key for entry in store
            ous[k] = pd.read_csv(os.path.join(pipeline_dir, cfg['relations'], rel+'.csv'))
            ous.get_storer(k).attrs['info'] = {'create':sql['create'],
                                               'insert':sql['insert'],
                                               'qcfg':qcfg}

        # Store the transformed instances
        for i,r in exponents.iterrows():
            exp = r['exponent']
            lg.info("xform::pow1st ::exponent %f"%exp)
            expk = 'exp/m%06d/'%(exp*1000)

            rel1 = True         # 1st relation in list?
            for rel,sql in qcfg['relations'].iteritems():
                lg.info("xform::pow1st ::exponent %f ::relation %s"%(exp,rel))
                df = pd.read_csv(os.path.join(pipeline_dir, cfg['relations'], rel+'.csv'))
                if rel1 == True: # If 1st relation, transform
                    df['conf'] = df.conf ** exp
                    rel1 = False
                k = expk+rel
                ous[k] = df
                ous.get_storer(k).attrs['info'] = {'create':sql['create'],
                                                   'insert':sql['insert'],
                                                   'qcfg':qcfg,
                                                   'numerator':r['numerator'],
                                                   'denominator':r['denominator']}
            
    finally:
        ous.close()

cmdline.run(options, checksum_level = rf.ruffus_utility.CHECKSUM_HISTORY_TIMESTAMPS, logger=lg)

