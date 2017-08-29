#!/bin/env python
"""
Usage:
    isdc_production [options]

Options:
    -o, --output DIR  output_directory [default: /gpfs0/fact/processing/gps_time]
    -i, --input DIR   input directory [default: /gpfs0/fact/processing/fad_counters/fad]
    --init            initialize the runinfo storage for this processing
"""
import os
import sys
from os.path import abspath
from os.path import join
from os.path import exists
from os.path import dirname
from datetime import datetime
import shutil
import pkg_resources
from shutil import which
import subprocess as sp
import pandas as pd
from tqdm import tqdm
from fact.path import TreePath
from fact.credentials import create_factdb_engine
from docopt import docopt
import logging
import numpy as np

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(message)s'
)

OBSERVATION_RUN_KEY = 1
SQL_QUERY = '''
SELECT
    fNight, fRunID, fNumEvents
FROM
    RunInfo
WHERE
    fRunTypeKey={0}
'''.format(OBSERVATION_RUN_KEY)


def main():
    args = docopt(__doc__)
    out_dir = abspath(args['--output'])
    input_dir = abspath(args['--input'])
    runstatus_path = join(out_dir, 'runinfo.csv')
    os.makedirs(out_dir, exist_ok=True)
    copy_top_level_readme_to(join(out_dir, 'README.md'))
    path_generators = init_path_generators(input_dir, out_dir)

    if args['--init']:
        if exists(runstatus_path):
            logging.error((
                'runstatus.csv already exists in out_dir.\n' +
                '%s\n' +
                'Running with --init would overwrite that. ' +
                'Please remove it yourself.'
                ).format(out_dir))
            sys.exit(-1)
        runstatus = initialize_runstatus()

    if not exists(runstatus_path):
        logging.error(
            'runstatus.csv file does not exist. Call with --init first')
        sys.exit(-1)

    runstatus = update_runstatus(runstatus_path)
    runstatus = check_for_input_files(runstatus, path_generators['input_file_path'])
    runstatus = check_for_output_files(runstatus, path_generators['output_file_path'])
    runstatus = check_length_of_output(runstatus, path_generators['output_file_path'])

    runs_not_yet_submitted = runstatus[
        runstatus.input_file_exists &
        (~runstatus.output_file_exists) &
        np.isnat(runstatus.submitted_at)
    ]

    for job in tqdm(
        runs_not_yet_submitted.itertuples(),
        desc='submitting:',
        total=len(runs_not_yet_submitted)
    ):
        qsub(job)
        runstatus.set_value(
            job.Index,
            'submitted_at',
            datetime.utcnow()
        )

    runstatus.to_csv(runstatus_path, 'all')


def init_path_generators(input_dir, out_dir):
    return {
        'input_file_path': TreePath(base_dir=input_dir, suffix='_fad.h5'),
        'std_out_path': TreePath(join(out_dir, 'std'), '.o'),
        'std_err_path': TreePath(join(out_dir, 'std'), '.e'),
        'output_file_path':
            TreePath(join(out_dir, 'gps_time'), '_gps_time.h5'),
        'models_path':
            TreePath(join(out_dir, 'gps_time_models'), '_models.h5'),
    }


def copy_top_level_readme_to(path):
    readme_res_path = pkg_resources.resource_filename(
        'fad_counters_to_gps_time',
        'gps_time_reconstruction/README.md'
    )
    shutil.copy(readme_res_path, path)


def initialize_runstatus():
    runinfo = pd.read_sql(SQL_QUERY, create_factdb_engine())
    runinfo['input_file_exists'] = False
    runinfo['output_file_exists'] = False
    runinfo['length_of_output'] = np.nan
    runinfo['submitted_at'] = pd.Timestamp('nat')

    return runinfo


def update_runstatus(path):
    logging.info('downloading list of observation runs ... ')
    runinfo = pd.read_sql(SQL_QUERY, create_factdb_engine())
    runinfo = runinfo.merge(
        pd.read_csv(path),
        on=list(runinfo.columns),
        how='outer',
    )
    runinfo.input_file_exists.fillna(False, inplace=True)
    runinfo.output_file_exists.fillna(False, inplace=True)
    return runinfo


def check_for_input_files(runinfo, path_generator):
    no_input_files = runinfo[~runinfo.input_file_exists]
    for job in tqdm(
            no_input_files.itertuples(),
            desc='check_for_input_file:',
            total=len(no_input_files)
    ):
        runinfo.set_value(
            job.Index,
            'input_file_exists',
            exists(path_generator(job))
        )
    return runinfo


def check_for_output_files(runinfo, path_generator):
    no_output_files = runinfo[~runinfo.output_file_exists]
    for job in tqdm(
            no_output_files.itertuples(),
            desc='check_for_ouput_file:',
            total=len(no_output_files)
    ):
        runinfo.set_value(
            job.Index,
            'output_file_exists',
            exists(path_generator(job))
        )
    return runinfo


def check_length_of_output(runinfo, path_generator):
    to_be_checked = runinfo[
        runinfo.output_file_exists & np.isnan(runinfo.length_of_output)
    ]
    for job in tqdm(
            to_be_checked.itertuples(),
            desc='check_output_length:',
            total=len(to_be_checked)
    ):
        try:
            runinfo.set_value(
                job.Index,
                'length_of_output',
                float(len(pd.read_hdf(path_generator(job))))
            )
        except KeyboardInterrupt:
            raise
        except:
            logging.exception('in check_length_of_output')

    return runinfo


def qsub(job, path_generators, queue='fact_medium'):
    os.makedirs(dirname(path_generators['std_out_path']), exist_ok=True)
    os.makedirs(dirname(path_generators['std_err_path']), exist_ok=True)

    cmd = [
        'qsub',
        '-q', queue,
        '-o', path_generators['std_out_path'](job),
        '-e', path_generators['std_err_path'](job),
        which('gps_time_reconstruction'),
        path_generators['input_file_path'](job),
        path_generators['output_file_path'](job),
        path_generators['models_path'](job),
    ]

    try:
        sp.check_output(cmd, stderr=sp.STDOUT)
    except sp.CalledProcessError as e:
        print('returncode', e.returncode)
        print('output', e.output)
        raise


if __name__ == '__main__':
    main()
