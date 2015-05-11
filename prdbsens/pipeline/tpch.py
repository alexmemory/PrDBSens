import sys
import os, errno
import glob
import ruffus as rf
import ruffus.cmdline as cmdline
import pandas as pd
import numpy as np
import os, errno
import yaml
# import logging
from trio import triodb
# import warnings
# warnings.filterwarnings("ignore", message=".*deprecation.*")

# Configuration and command line options
parser = cmdline.get_argparse(description='Pipeline using the TPC-H example.')
parser.add_argument("--config")
options = parser.parse_args()
lg, lm = cmdline.setup_logging (__name__, options.log_file, options.verbose)
# lg.setLevel(logging.INFO)
if vars(options)['config'] == None:
    print "No config supplied."
    parser.print_help()
    sys.exit()
with open(vars(options)['config'], 'r') as f: cfg = yaml.load(f)
lg.info('pipeline:: ::config %s'%str(cfg))

SQL_PRINT_MAX=1000

# =============================================================
#                    functions
# =============================================================

def execute_sql_commands(sql, cur):
    """Execute multiple SQL commands in a string on a cursor."""
    for line in sql.split(";"):
        line = line.strip()
        line = line.replace("\n"," ")
        if line == "":
            continue
        # lg.info("sql:: ::line %s"%line)
        cur.execute(line)
        
def q(sql):
    return sql.replace("\n"," ").replace(";","")

# =============================================================
#                    pipeline
# =============================================================

pipeline_dir = os.path.join(cfg['pipelines'],cfg['name'])

# =============================================================
#                    sampling
# =============================================================

rel_orig_dir = os.path.join(pipeline_dir, cfg['relations_orig'])

# Input query file paths
path_glob = os.path.join(rel_orig_dir, '*.csv')
rel_orig_paths = glob.glob(path_glob)
max_rows = cfg['relation_sampling']['max_rows']

@rf.mkdir(rel_orig_paths, rf.formatter(), "{path[0]}/sam/%d"%max_rows)
@rf.transform(rel_orig_paths, rf.formatter(),
              "{path[0]}/sam/%d/{basename[0]}{ext[0]}"%max_rows, lg, lm,"{basename[0]}")
def rel_sam(in_path, out_path, lg, lm, fname):
    """Sample relation files if necessary."""

    lg.info("rel_orig::%s sampling:: ::starting"%fname)
    lg.info("rel_orig::%s sampling:: ::in_path %s"%(fname,in_path))
    df = pd.read_csv(in_path, quotechar="'")

    if len(df) > max_rows:
        df = df.ix[np.random.choice(df.index, max_rows, replace=False)]
    
    df.to_csv(out_path, index=False, quotechar="'")
    lg.info("rel_orig::%s sampling:: ::done"%fname)

# =============================================================
#                    pow, 1st
# =============================================================

transform_dir = os.path.join(pipeline_dir, cfg['transform']['pow']['1st']['dir'])

# Input query file paths
path_glob = os.path.join(transform_dir, 'q', '*.yaml')
q_paths = glob.glob(path_glob)

# Input pairs the exponents list with each query file
exp_path = os.path.join(pipeline_dir, cfg['transform']['pow']['exponents'])
in_paths = [[q_path, exp_path] for q_path in q_paths]

@rf.follows(rel_sam)
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
            ous[k] = pd.read_csv(os.path.join(pipeline_dir, cfg['relations'], rel+'.csv'),
                                 quotechar="'")
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
                df = pd.read_csv(os.path.join(pipeline_dir, cfg['relations'], rel+'.csv'),
                                 quotechar="'")
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

@rf.jobs_limit(1)               # For now, prevent parallel tasks using Trio
@rf.mkdir(pow1st, rf.formatter(), "{path[0]}/qres")
@rf.transform(pow1st, rf.formatter(),
              "{path[0]}/qres/{basename[0]}{ext[0]}", lg, lm)
def pow1st_qres(in_path, out_path, lg, lm):
    """Write transformed instances to a database and execute the query."""

    ins = pd.HDFStore(in_path,'r') 
    ous = pd.HDFStore(out_path,'w') 
    try:
        qcfg = ins.get_storer(ins.keys()[0]).attrs.info['qcfg']
        lg.info("xform::pow1st_qres ::qcfg %s"%qcfg)
        
        # Query the non-transformed instance
        conn = triodb.connect(database=cfg['database']['name'], 
                              user=cfg['database']['user'], 
                              password=cfg['database']['password'])
        cur = conn.cursor()
        try:
            for rel,k in [(grp._v_name,grp._v_pathname) for grp in ins.root.orig]:
                lg.info("xform::pow1st_qres path::%s ::starting"%k)
                t = ins[k]          # dataframe with relation
                info = ins.get_storer(k).attrs.info

                sqls = [info['create']] # SQL to create table
                for r in t.itertuples(): # 1st col is index, last is confidence
                    sqls.append(info['insert'] %r[1:]) # SQL to insert a row
                sql = '\n'.join(sqls)
                execute_sql_commands(sql, cur)
                # lg.info("xform::pow1st_qres path::%s ::sql %s"%(k,sql))
                lg.info("xform::pow1st_qres path::%s ::done"%k)

            sql = qcfg['create'] # Create the table to query
            lg.info("xform::pow1st_qres table:: ::sql %s"%sql)
            cur.execute(q(sql))
            # for t in cur.xfetchall(): print t
            
            sql = qcfg['query'] # The query with the ranking we care about
            lg.info("xform::pow1st_qres query:: ::sql %s"%sql)
            cur.execute(q(sql))

            rows = []           # Rows of the result of the query
            for t in cur.xfetchall(): 
                rows.append({'tuple':str(t).strip(),
                             'conf':t.alternatives[0].computeConfidence(conn)})
            df = pd.DataFrame(rows)
            lg.info("xform::pow1st_qres query:: ::df %s"%('\n'+str(df.head(2))))

            newk = 'orig'
            ous[newk] = df                            # Store result in output df
            ous.get_storer(newk).attrs['info'] = info # Arbitrarily copy an info

        finally:
            conn.close()

        # Query the transformed instances
        for xfnam,xfdir in [(grp._v_name,grp._v_pathname) for grp in ins.root.exp]:
            lg.info("xform::pow1st_qres xfname::%s ::starting"%xfnam)

            conn = triodb.connect(database=cfg['database']['name'], 
                                  user=cfg['database']['user'], 
                                  password=cfg['database']['password'])
            cur = conn.cursor()
            try:
                for rel,k in [(grp._v_name,grp._v_pathname) for
                              grp in ins.root.exp._f_get_child(xfnam)]:
                    lg.info("xform::pow1st_qres path::%s ::starting"%k)
                    t = ins[k]          # dataframe with relation
                    info = ins.get_storer(k).attrs.info

                    sqls = [info['create']] # SQL to create table
                    for r in t.itertuples(): # 1st col is index, last is confidence
                        sqls.append(info['insert'] %r[1:]) # SQL to insert a row
                    sql = '\n'.join(sqls)
                    execute_sql_commands(sql, cur)
                    lg.info("xform::pow1st_qres path::%s ::done"%k)

                sql = qcfg['create'] # Create the table to query
                lg.info("xform::pow1st_qres table:: ::sql %s"%sql)
                cur.execute(q(sql))
                # for t in cur.xfetchall(): print t

                sql = qcfg['query'] # The query
                lg.info("xform::pow1st_qres query:: ::sql %s"%sql)
                cur.execute(q(sql))

                rows = []
                for t in cur.xfetchall(): 
                    rows.append({'tuple':str(t).strip(),
                                 'conf':t.alternatives[0].computeConfidence(conn)})
                df = pd.DataFrame(rows)
                lg.info("xform::pow1st_qres query:: ::df %s"%('\n'+str(df.head(2))))

                newk = xfdir
                ous[newk] = df
                ous.get_storer(newk).attrs['info'] = info # Arbitrarily copy an info

            finally:
                conn.close()
                
            lg.info("xform::pow1st_qres xfname::%s ::done"%xfnam)
    finally:
        ins.close()
        ous.close()

@rf.mkdir(pow1st_qres, rf.formatter(), "{path[0]}/cmp")
@rf.transform(pow1st_qres, rf.formatter(),
              "{path[0]}/cmp/{basename[0]}{ext[0]}", lg, lm)
def pow1st_cmp(in_path, out_path, lg, lm):
    """Compare query results of transformed db instances to non-transformed."""

    ins = pd.HDFStore(in_path,'r') 
    ous = pd.HDFStore(out_path,'w') 
    try:
        qcfg = ins.get_storer(ins.keys()[0]).attrs.info['qcfg']
        lg.info("xform::pow1st_cmp ::qcfg %s"%qcfg)
        
        dfo = ins['orig']      # Query result of non-transformed instance
        assert len(dfo) > 1
        dfo = dfo.set_index('tuple').rename(columns={'conf':'orig'})
        dfo['origRnk'] = dfo.rank(ascending=False) # Rank by desc. conf.

        results = []
        # Loop over transformed results
        for xfnam,xfpath in [(grp._v_name,grp._v_pathname) for grp in ins.root.exp]:
            lg.info("xform::pow1st_cmp path::%s ::starting"%xfpath)
            dft = ins[xfpath]   # Query result of a transformed instance
            assert len(dft) > 1
            info = ins.get_storer(xfpath).attrs.info

            dft = dft.set_index('tuple').rename(columns={'conf':'xform'})
            dft['xformRnk'] = dft.rank(ascending=False) # By desc conf

            dfm = dfo.join(dft) # Merge in preparation for comparison
            assert len(dfo) == len(dft) == len(dfm)
            results.append({'numerator':info['numerator'],
                            'denominator':info['denominator'],
                            'pearson':dfm.corr(method='pearson').loc['orig']['xform'],
                            'spearman':dfm.corr(method='spearman').loc['origRnk']['xformRnk'],
                            'kendall':dfm.corr(method='kendall').loc['origRnk']['xformRnk']})
            lg.info("xform::pow1st_cmp path::%s ::done"%xfpath)

        newk = 'comparison'
        ous[newk] = pd.DataFrame(results).set_index(['numerator','denominator'])
        ous.get_storer(newk).attrs.info = ins.get_storer('orig').attrs.info

    finally:
        ins.close()
        ous.close()

# =============================================================
#                    pow, all
# =============================================================

transform_dir = os.path.join(pipeline_dir, cfg['transform']['pow']['all']['dir'])

# Input query file paths
path_glob = os.path.join(transform_dir, 'q', '*.yaml')
q_paths = glob.glob(path_glob)

# Input pairs the exponents list with each query file
exp_path = os.path.join(pipeline_dir, cfg['transform']['pow']['exponents'])
in_paths = [[q_path, exp_path] for q_path in q_paths]

@rf.follows(rel_sam)
@rf.mkdir(in_paths, rf.formatter(), "{path[0]}/{basename[0]}")
@rf.transform(in_paths, rf.formatter(),
              "{path[0]}/{basename[0]}/{basename[0]}.h5", lg, lm)
def powall(in_path, out_path, lg, lm):
    """Load inputs and prepare transformed instances."""

    q_path,exp_path = in_path   # query file and file listing exponents
    lg.info("xform::powall ::out_path %s"%out_path)
    lg.info("xform::powall ::q_file %s"%q_path)
    lg.info("xform::powall ::exponents_file %s"%exp_path)
    
    exponents = pd.read_csv(exp_path, comment="#")
    exponents['exponent'] = 1.*exponents.numerator/exponents.denominator

    with open(q_path,'r') as f: qcfg = yaml.load(f) # query config

    ous = pd.HDFStore(out_path,'w') # To hold instances, some xformed
    try:
        # Store the non-transformed instance
        for rel,sql in qcfg['relations'].iteritems():
            lg.info("xform::powall ::relation %s"%rel)
            k = 'orig/'+rel # key for entry in store
            t = pd.read_csv(os.path.join(pipeline_dir, cfg['relations'], rel+'.csv'),
                                 quotechar="'")
            lg.info("xform::powall relation::%s ::dtypes %s"%(rel,t.dtypes))
            # t = t.apply(lambda c: c.str.replace(';',' '), axis=1) # Avoid errors
            # lg.info("xform::powall relation::%s ::cleaned"%(rel))
            # lg.info("xform::powall relation::%s ::dtypes %s"%(rel,t.dtypes))
            ous[k] = t
            ous.get_storer(k).attrs['info'] = {'create':sql['create'],
                                               'insert':sql['insert'],
                                               'qcfg':qcfg}

        # Store the transformed instances
        for i,r in exponents.iterrows():
            exp = r['exponent']
            lg.info("xform::powall ::exponent %f"%exp)
            expk = 'exp/m%06d/'%(exp*1000)

            for rel,sql in qcfg['relations'].iteritems():
                lg.info("xform::powall exponent::%f ::relation %s"%(exp,rel))
                df = pd.read_csv(os.path.join(pipeline_dir, cfg['relations'], rel+'.csv'),
                                 quotechar="'")
                lg.info("exponent::%f relation::%s ::dtypes %s"%(exp,rel,df.dtypes))
                # df = df.apply(lambda c: c.str.replace(';',' '), axis=1) # Avoid errors
                # lg.info("exponent::%f relation::%s ::cleaned"%(exp,rel))
                # lg.info("exponent::%f relation::%s ::dtypes %s"%(exp,rel,df.dtypes))
                df['conf'] = df.conf ** exp
                k = expk+rel
                ous[k] = df
                ous.get_storer(k).attrs['info'] = {'create':sql['create'],
                                                   'insert':sql['insert'],
                                                   'qcfg':qcfg,
                                                   'numerator':r['numerator'],
                                                   'denominator':r['denominator']}
            
    finally:
        ous.close()

@rf.jobs_limit(1)               # For now, prevent parallel tasks using Trio
@rf.mkdir(powall, rf.formatter(), "{path[0]}/qres")
@rf.transform(powall, rf.formatter(),
              "{path[0]}/qres/{basename[0]}{ext[0]}", lg, lm)
def powall_qres(in_path, out_path, lg, lm):
    """Write transformed instances to a database and execute the query."""

    ins = pd.HDFStore(in_path,'r') 
    ous = pd.HDFStore(out_path,'w') 
    try:
        qcfg = ins.get_storer(ins.keys()[0]).attrs.info['qcfg']
        lg.info("xform::powall_qres ::qcfg %s"%qcfg)
        
        lg.info("xform::powall_qres orig_instances:: ::starting")

        # Query the non-transformed instance
        conn = triodb.connect(database=cfg['database']['name'], 
                              user=cfg['database']['user'], 
                              password=cfg['database']['password'])
        cur = conn.cursor()
        try:
            for rel,k in [(grp._v_name,grp._v_pathname) for grp in ins.root.orig]:
                lg.info("xform::powall_qres path::%s ::starting"%k)
                t = ins[k]          # dataframe with relation
                info = ins.get_storer(k).attrs.info

                sqls = [info['create']] # SQL to create table
                for r in t.itertuples(): # 1st col is index, last is confidence
                    sqls.append(info['insert'] %r[1:]) # SQL to insert a row
                sql = '\n'.join(sqls)
                lg.info("xform::powall_qres path::%s ::sql %s"%(k,sql[-min(len(sql),SQL_PRINT_MAX):]))
                execute_sql_commands(sql, cur)
                lg.info("xform::powall_qres path::%s ::done"%k)

            sql = qcfg['create'] # Create the table to query
            lg.info("xform::powall_qres table:: ::sql %s"%sql)
            cur.execute(q(sql))
            # try:
            #     for t in cur.xfetchall():
            #         print t  # Throws error with distinct, for some reason?
            # except Exception as e:
            #     lg.warning(e)
            
            sql = qcfg['query'] # The query with the ranking we care about
            lg.info("xform::powall_qres query:: ::sql %s"%sql)
            cur.execute(q(sql))

            rows = []           # Rows of the result of the query
            for t in cur.xfetchall(): 
                rows.append({'tuple':str(t).strip(),
                             'conf':t.alternatives[0].computeConfidence(conn)})
            df = pd.DataFrame(rows)
            lg.info("xform::powall_qres query:: ::df %s"%('\n'+str(df.head(2))))

            newk = 'orig'
            ous[newk] = df                            # Store result in output df
            ous.get_storer(newk).attrs['info'] = info # Arbitrarily copy an info

        finally:
            conn.close()
        lg.info("xform::powall_qres orig_instances:: ::done")

        # Query the transformed instances
        lg.info("xform::powall_qres xform_instances:: ::starting")
        for xfnam,xfdir in [(grp._v_name,grp._v_pathname) for grp in ins.root.exp]:
            lg.info("xform::powall_qres xfname::%s ::starting"%xfnam)

            conn = triodb.connect(database=cfg['database']['name'], 
                                  user=cfg['database']['user'], 
                                  password=cfg['database']['password'])
            cur = conn.cursor()
            try:
                for rel,k in [(grp._v_name,grp._v_pathname) for
                              grp in ins.root.exp._f_get_child(xfnam)]:
                    lg.info("xform::powall_qres path::%s ::starting"%k)
                    t = ins[k]          # dataframe with relation
                    info = ins.get_storer(k).attrs.info

                    sqls = [info['create']] # SQL to create table
                    for r in t.itertuples(): # 1st col is index, last is confidence
                        sqls.append(info['insert'] %r[1:]) # SQL to insert a row
                    sql = '\n'.join(sqls)
                    lg.info("xform::powall_qres path::%s ::sql %s"%(k,sql[-min(len(sql),SQL_PRINT_MAX):]))
                    execute_sql_commands(sql, cur)
                    lg.info("xform::powall_qres path::%s ::done"%k)

                sql = qcfg['create'] # Create the table to query
                lg.info("xform::powall_qres table:: ::sql %s"%sql)
                cur.execute(q(sql))
                # try:
                #     for t in cur.xfetchall():
                #         print t  # Throws error with distinct, for some reason?
                # except Exception as e:
                #     lg.warning(e)

                sql = qcfg['query'] # The query
                lg.info("xform::powall_qres query:: ::sql %s"%sql)
                cur.execute(q(sql))

                rows = []
                for t in cur.xfetchall(): 
                    rows.append({'tuple':str(t).strip(),
                                 'conf':t.alternatives[0].computeConfidence(conn)})
                df = pd.DataFrame(rows)
                lg.info("xform::powall_qres query:: ::df %s"%('\n'+str(df.head(2))))

                newk = xfdir
                ous[newk] = df
                ous.get_storer(newk).attrs['info'] = info # Arbitrarily copy an info

            finally:
                conn.close()
                
            lg.info("xform::powall_qres xfname::%s ::done"%xfnam)
        lg.info("xform::powall_qres xform_instances:: ::done")
    finally:
        ins.close()
        ous.close()

@rf.mkdir(powall_qres, rf.formatter(), "{path[0]}/cmp")
@rf.transform(powall_qres, rf.formatter(),
              "{path[0]}/cmp/{basename[0]}{ext[0]}", lg, lm)
def powall_cmp(in_path, out_path, lg, lm):
    """Compare query results of transformed db instances to non-transformed."""

    ins = pd.HDFStore(in_path,'r') 
    ous = pd.HDFStore(out_path,'w') 
    try:
        qcfg = ins.get_storer(ins.keys()[0]).attrs.info['qcfg']
        lg.info("xform::powall_cmp ::qcfg %s"%qcfg)
        
        dfo = ins['orig']      # Query result of non-transformed instance
        assert len(dfo) > 1
        dfo = dfo.set_index('tuple').rename(columns={'conf':'orig'})
        dfo['origRnk'] = dfo.rank(ascending=False) # Rank by desc. conf.

        results = []
        # Loop over transformed results
        for xfnam,xfpath in [(grp._v_name,grp._v_pathname) for grp in ins.root.exp]:
            lg.info("xform::powall_cmp path::%s ::starting"%xfpath)
            dft = ins[xfpath]   # Query result of a transformed instance
            assert len(dft) > 1
            info = ins.get_storer(xfpath).attrs.info

            dft = dft.set_index('tuple').rename(columns={'conf':'xform'})
            dft['xformRnk'] = dft.rank(ascending=False) # By desc conf

            dfm = dfo.join(dft) # Merge in preparation for comparison
            assert len(dfo) == len(dft) == len(dfm)
            results.append({'numerator':info['numerator'],
                            'denominator':info['denominator'],
                            'pearson':dfm.corr(method='pearson').loc['orig']['xform'],
                            'spearman':dfm.corr(method='spearman').loc['origRnk']['xformRnk'],
                            'kendall':dfm.corr(method='kendall').loc['origRnk']['xformRnk']})
            lg.info("xform::powall_cmp path::%s ::done"%xfpath)

        newk = 'comparison'
        ous[newk] = pd.DataFrame(results).set_index(['numerator','denominator'])
        ous.get_storer(newk).attrs.info = ins.get_storer('orig').attrs.info

    finally:
        ins.close()
        ous.close()
        
cmdline.run(options, checksum_level = rf.ruffus_utility.CHECKSUM_HISTORY_TIMESTAMPS, logger=lg)
